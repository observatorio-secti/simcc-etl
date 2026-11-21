"""Testes de integração para remove_researchers com exclusão em cascata."""

import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

pytest.importorskip('testcontainers.postgres')
from testcontainers.postgres import PostgresContainer  # noqa: E402

from simcc_etl.routines.remove_researchers import (  # noqa: E402
    run as run_remove_researchers,
)

pytestmark = pytest.mark.integration

SCHEMA_SQL = """
CREATE TABLE public.researcher (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    lattes_id text
);

CREATE TABLE public.research_group (
    id uuid PRIMARY KEY,
    first_leader_id uuid,
    second_leader_id uuid
);

CREATE TABLE public.bibliographic_production (
    id uuid PRIMARY KEY,
    researcher_id uuid NOT NULL
        REFERENCES public.researcher(id) ON DELETE CASCADE,
    title text NOT NULL
);

CREATE TABLE public.bibliographic_production_article (
    id uuid PRIMARY KEY,
    bibliographic_production_id uuid NOT NULL
        REFERENCES public.bibliographic_production(id)
);

CREATE TABLE public.bibliographic_production_book (
    id uuid PRIMARY KEY,
    bibliographic_production_id uuid NOT NULL
        REFERENCES public.bibliographic_production(id)
);

CREATE TABLE public.bibliographic_production_book_chapter (
    id uuid PRIMARY KEY,
    bibliographic_production_id uuid NOT NULL
        REFERENCES public.bibliographic_production(id)
);

CREATE TABLE public.bibliographic_production_work_in_event (
    bibliographic_production_id uuid PRIMARY KEY
        REFERENCES public.bibliographic_production(id)
);

CREATE TABLE public.process_or_technique (
    id uuid PRIMARY KEY,
    researcher_id uuid NOT NULL
        REFERENCES public.researcher(id) ON DELETE CASCADE
);

CREATE TABLE public.process_author (
    id uuid PRIMARY KEY,
    process_id uuid NOT NULL REFERENCES public.process_or_technique(id)
);

CREATE TABLE public.process_keyword (
    id uuid PRIMARY KEY,
    process_id uuid NOT NULL REFERENCES public.process_or_technique(id)
);

CREATE TABLE public.process_knowledge_area (
    id uuid PRIMARY KEY,
    process_id uuid NOT NULL REFERENCES public.process_or_technique(id)
);

CREATE TABLE public.research_project (
    id uuid PRIMARY KEY,
    researcher_id uuid NOT NULL
        REFERENCES public.researcher(id) ON DELETE CASCADE
);

CREATE TABLE public.research_project_components (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES public.research_project(id)
);

CREATE TABLE public.research_project_foment (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES public.research_project(id)
);

CREATE TABLE public.research_project_production (
    project_id uuid PRIMARY KEY REFERENCES public.research_project(id)
);
"""


@pytest.fixture(scope='module')
def pg_engine():
    with PostgresContainer('pgvector/pgvector:pg17', driver='psycopg') as pg:
        engine = sa.create_engine(pg.get_connection_url())
        with engine.begin() as conn:
            conn.execute(sa.text(SCHEMA_SQL))
        yield engine
        engine.dispose()


def _seed_researcher(conn: sa.Connection, lattes_id: str) -> uuid.UUID:
    rid = uuid.uuid4()
    conn.execute(
        sa.text(
            'INSERT INTO public.researcher (id, name, lattes_id) '
            'VALUES (:id, :name, :lattes_id)'
        ),
        {'id': rid, 'name': f'Researcher {lattes_id}', 'lattes_id': lattes_id},
    )

    for subtable in (
        'bibliographic_production_article',
        'bibliographic_production_book',
        'bibliographic_production_book_chapter',
    ):
        bp_id = uuid.uuid4()
        conn.execute(
            sa.text(
                'INSERT INTO public.bibliographic_production '
                '(id, researcher_id, title) VALUES (:id, :rid, :title)'
            ),
            {'id': bp_id, 'rid': rid, 'title': f'Title {subtable}'},
        )
        conn.execute(
            sa.text(
                f'INSERT INTO public.{subtable} '
                '(id, bibliographic_production_id) VALUES (:id, :bp_id)'
            ),
            {'id': uuid.uuid4(), 'bp_id': bp_id},
        )

    bp_event_id = uuid.uuid4()
    conn.execute(
        sa.text(
            'INSERT INTO public.bibliographic_production '
            '(id, researcher_id, title) VALUES (:id, :rid, :title)'
        ),
        {'id': bp_event_id, 'rid': rid, 'title': 'Work in event'},
    )
    conn.execute(
        sa.text(
            'INSERT INTO public.bibliographic_production_work_in_event '
            '(bibliographic_production_id) VALUES (:bp_id)'
        ),
        {'bp_id': bp_event_id},
    )

    proc_id = uuid.uuid4()
    conn.execute(
        sa.text(
            'INSERT INTO public.process_or_technique (id, researcher_id) '
            'VALUES (:id, :rid)'
        ),
        {'id': proc_id, 'rid': rid},
    )
    for subtable in (
        'process_author',
        'process_keyword',
        'process_knowledge_area',
    ):
        conn.execute(
            sa.text(
                f'INSERT INTO public.{subtable} (id, process_id) '
                'VALUES (:id, :proc_id)'
            ),
            {'id': uuid.uuid4(), 'proc_id': proc_id},
        )

    proj_id = uuid.uuid4()
    conn.execute(
        sa.text(
            'INSERT INTO public.research_project (id, researcher_id) '
            'VALUES (:id, :rid)'
        ),
        {'id': proj_id, 'rid': rid},
    )
    for subtable in ('research_project_components', 'research_project_foment'):
        conn.execute(
            sa.text(
                f'INSERT INTO public.{subtable} (id, project_id) '
                'VALUES (:id, :proj_id)'
            ),
            {'id': uuid.uuid4(), 'proj_id': proj_id},
        )
    conn.execute(
        sa.text(
            'INSERT INTO public.research_project_production (project_id) '
            'VALUES (:proj_id)'
        ),
        {'proj_id': proj_id},
    )
    return rid


def test_remove_researchers_with_nested_production_and_project_children(
    pg_engine,
):
    with pg_engine.begin() as conn:
        r_remove = _seed_researcher(conn, '1111111111111111')
        r_keep = _seed_researcher(conn, '2222222222222222')
        group_id = uuid.uuid4()
        conn.execute(
            sa.text(
                'INSERT INTO public.research_group '
                '(id, first_leader_id, second_leader_id) '
                'VALUES (:gid, :r1, :r1)'
            ),
            {'gid': group_id, 'r1': r_remove},
        )

    with Session(pg_engine) as session:
        stats = run_remove_researchers(
            session,
            researcher_ids=[str(r_remove)],
        )

    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0

    with pg_engine.connect() as conn:
        remaining = conn.execute(
            sa.text('SELECT id FROM public.researcher')
        ).fetchall()
        assert [r[0] for r in remaining] == [r_keep]

        leaders = conn.execute(
            sa.text(
                'SELECT first_leader_id, second_leader_id '
                'FROM public.research_group WHERE id = :gid'
            ),
            {'gid': group_id},
        ).one()
        assert leaders == (None, None)

        for table in (
            'bibliographic_production',
            'bibliographic_production_article',
            'bibliographic_production_book',
            'bibliographic_production_book_chapter',
            'bibliographic_production_work_in_event',
            'process_or_technique',
            'process_author',
            'process_keyword',
            'process_knowledge_area',
            'research_project',
            'research_project_components',
            'research_project_foment',
            'research_project_production',
        ):
            count = conn.execute(
                sa.text(f'SELECT COUNT(*) FROM public.{table}')
            ).scalar()
            expected = 4 if table == 'bibliographic_production' else 1
            assert count == expected, f'{table} count={count}'
