"""Publicação real entre dois bancos Postgres num container descartável."""

import threading
import time

import psycopg
import pytest

pytest.importorskip('testcontainers.postgres')
from testcontainers.postgres import PostgresContainer  # noqa: E402

from simcc_etl.routines import publish_to_read_db as pub  # noqa: E402

pytestmark = pytest.mark.integration

STAGING_EXISTS_SQL = (
    "SELECT 1 FROM pg_namespace WHERE nspname = 'simcc_publish_staging'"
)

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE FUNCTION f_upper(text) RETURNS text
    LANGUAGE sql IMMUTABLE AS 'SELECT upper($1)';
CREATE TABLE researcher (
    id integer PRIMARY KEY,
    name text NOT NULL CHECK (name <> '')
);
CREATE INDEX idx_researcher_name ON researcher USING gin (name gin_trgm_ops);
CREATE TABLE article (
    id integer PRIMARY KEY,
    researcher_id integer NOT NULL
        REFERENCES researcher(id) ON DELETE CASCADE,
    title text,
    year integer,
    UNIQUE (researcher_id, title, year)
);
CREATE TABLE etl_runs (id integer PRIMARY KEY);
CREATE MATERIALIZED VIEW mv_articles AS
    SELECT a.id, f_upper(a.title) AS title, r.name
    FROM article a JOIN researcher r ON r.id = a.researcher_id;
CREATE UNIQUE INDEX uq_mv_articles ON mv_articles (id);
CREATE MATERIALIZED VIEW mv_article_count AS
    SELECT name, count(*) AS total FROM mv_articles GROUP BY name;
CREATE UNIQUE INDEX uq_mv_article_count ON mv_article_count (name);
"""


@pytest.fixture(scope='module')
def server():
    with PostgresContainer('pgvector/pgvector:pg17', driver=None) as pg:
        admin = pg.get_connection_url()
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute("CREATE ROLE api_reader LOGIN PASSWORD 'x'")
        yield admin


@pytest.fixture
def dbs(server, request):
    base = server.rsplit('/', 1)[0]
    suffix = request.node.name.replace('[', '_').replace(']', '')[-40:]
    write_name, read_name = f'w_{suffix}', f'r_{suffix}'
    with psycopg.connect(server, autocommit=True) as conn:
        for name in (write_name, read_name):
            conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
            conn.execute(f'CREATE DATABASE "{name}"')
    write_dsn, read_dsn = f'{base}/{write_name}', f'{base}/{read_name}'
    for dsn in (write_dsn, read_dsn):
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(SCHEMA)

    with psycopg.connect(write_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO researcher VALUES (1, 'Ana'), (2, 'Bruno');"
            "INSERT INTO article VALUES (1, 1, 'novo', 2025),"
            " (2, 1, 'outro', 2025), (3, 2, 'mais um', 2024);"
            'INSERT INTO etl_runs VALUES (99);'
        )
    with psycopg.connect(read_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO researcher VALUES (9, 'Antigo');"
            "INSERT INTO article VALUES (9, 9, 'velho', 2020);"
            'INSERT INTO etl_runs VALUES (1);'
            'REFRESH MATERIALIZED VIEW mv_articles;'
            'REFRESH MATERIALIZED VIEW mv_article_count;'
            'GRANT SELECT ON article, mv_articles TO api_reader;'
        )
    return write_dsn, read_dsn


def _fetch(dsn, query):
    with psycopg.connect(dsn) as conn:
        return conn.execute(query).fetchall()


def test_publish_replaces_data_and_keeps_structure(dbs):
    write_dsn, read_dsn = dbs

    result = pub.publish_via_staging(write_dsn, read_dsn)

    assert (result.tables, result.mviews) == (2, 2)
    assert _fetch(read_dsn, 'SELECT id FROM article ORDER BY id') == [
        (1,),
        (2,),
        (3,),
    ]
    assert _fetch(
        read_dsn, 'SELECT name, total FROM mv_article_count ORDER BY name'
    ) == [('Ana', 2), ('Bruno', 1)]
    assert _fetch(read_dsn, 'SELECT title FROM mv_articles WHERE id = 1') == [
        ('NOVO',)
    ]
    # Tabelas excluídas não são tocadas.
    assert _fetch(read_dsn, 'SELECT id FROM etl_runs') == [(1,)]

    constraints = {
        r[0]
        for r in _fetch(
            read_dsn,
            'SELECT conname FROM pg_constraint '
            "WHERE conrelid IN ('public.article'::regclass,"
            " 'public.researcher'::regclass)",
        )
    }
    assert {
        'article_pkey',
        'article_researcher_id_fkey',
        'article_researcher_id_title_year_key',
        'researcher_pkey',
        'researcher_name_check',
    } <= constraints

    indexes = {
        r[0]
        for r in _fetch(
            read_dsn,
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
        )
    }
    assert {
        'idx_researcher_name',
        'uq_mv_articles',
        'uq_mv_article_count',
    } <= indexes

    grants = _fetch(
        read_dsn,
        "SELECT relname FROM pg_class WHERE relnamespace = 'public'::"
        "regnamespace AND relkind IN ('r', 'm') AND "
        "has_table_privilege('api_reader', oid, 'SELECT') ORDER BY 1",
    )
    assert grants == [('article',), ('mv_articles',)]

    assert _fetch(read_dsn, STAGING_EXISTS_SQL) == []
    # MVs continuam atualizáveis de forma concorrente após a troca.
    assert pub.refresh_materialized_views(read_dsn) == 2


def test_schema_drift_aborts_without_touching_read_db(dbs):
    write_dsn, read_dsn = dbs
    with psycopg.connect(write_dsn, autocommit=True) as conn:
        conn.execute('ALTER TABLE article ADD COLUMN doi text')

    with pytest.raises(pub.SchemaMismatchError, match='article.doi'):
        pub.publish_via_staging(write_dsn, read_dsn)

    assert _fetch(read_dsn, 'SELECT id FROM article') == [(9,)]


def test_fk_violation_aborts_without_touching_read_db(dbs):
    write_dsn, read_dsn = dbs
    with psycopg.connect(write_dsn, autocommit=True) as conn:
        conn.execute(
            'ALTER TABLE article DROP CONSTRAINT article_researcher_id_fkey;'
            "INSERT INTO article VALUES (50, 777, 'órfão', 2025);"
        )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        pub.publish_via_staging(write_dsn, read_dsn)

    assert _fetch(read_dsn, 'SELECT id FROM article') == [(9,)]
    assert _fetch(read_dsn, STAGING_EXISTS_SQL) == []


def test_swap_gives_up_when_readers_hold_locks(dbs, monkeypatch):
    write_dsn, read_dsn = dbs
    monkeypatch.setattr(pub, 'SWAP_LOCK_TIMEOUT', '200ms')
    monkeypatch.setattr(pub, 'SWAP_ATTEMPTS', 2)
    monkeypatch.setattr(pub, 'SWAP_RETRY_DELAY_S', 0.1)

    holding = threading.Event()
    release = threading.Event()

    def long_reader():
        with psycopg.connect(read_dsn) as conn:
            conn.execute('SELECT count(*) FROM article')
            holding.set()
            release.wait(30)

    reader = threading.Thread(target=long_reader)
    reader.start()
    holding.wait(10)
    try:
        started = time.monotonic()
        with pytest.raises(pub.PublishError, match='locks da troca'):
            pub.publish_via_staging(write_dsn, read_dsn)
        assert time.monotonic() - started < 20
    finally:
        release.set()
        reader.join()

    assert _fetch(read_dsn, 'SELECT id FROM article') == [(9,)]
    assert _fetch(read_dsn, STAGING_EXISTS_SQL) == []
