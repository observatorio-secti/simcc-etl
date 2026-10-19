import psycopg
import redis
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.publish_to_read_db')

SEARCH_MATERIALIZED_VIEWS = [
    'mv_search_articles',
    'mv_search_books',
    'mv_search_patents',
    'mv_search_software',
    'mv_search_events',
    'mv_search_areas',
    'mv_search_documents',
    'mv_researcher_search',
    'mv_canonical_articles',
    'mv_canonical_books',
    'mv_canonical_book_chapters',
    'mv_canonical_software',
    'mv_canonical_patents',
    'mv_canonical_events',
    'mv_canonical_research_projects',
]

EXCLUDED_TABLES = {
    'alembic_version',
    'alembic_version_etl',
    'etl_runs',
}

REDIS_GENERATION_KEY = 'simcc:v2:search:generation'


def _clean_dsn(url: str) -> str:
    """Converte URL SQLAlchemy em string DSN compatível com psycopg."""
    if url.startswith('postgresql+psycopg://'):
        return url.replace('postgresql+psycopg://', 'postgresql://', 1)
    if url.startswith('postgresql+asyncpg://'):
        return url.replace('postgresql+asyncpg://', 'postgresql://', 1)
    return url


def _get_copyable_tables(conn: psycopg.Connection) -> list[str]:
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        tables = [row[0] for row in cur.fetchall()]
    return [t for t in tables if t not in EXCLUDED_TABLES]


def _copy_table_binary(
    write_conn: psycopg.Connection,
    read_conn: psycopg.Connection,
    table: str,
) -> None:
    logger.debug(f'Copying table {table} from write_db to read_db')
    with (
        read_conn.cursor() as r_cur,
        write_conn.cursor() as w_cur,
    ):
        copy_out_sql = f'COPY "{table}" TO STDOUT (FORMAT BINARY)'
        copy_in_sql = f'COPY "{table}" FROM STDIN (FORMAT BINARY)'
        with (
            w_cur.copy(copy_out_sql) as copy_out,
            r_cur.copy(copy_in_sql) as copy_in,
        ):
            for data in copy_out:
                copy_in.write(data)


def copy_tables_to_read_db(
    write_dsn: str,
    read_dsn: str,
) -> int:
    """Copia tabelas via binary COPY do write_db para o read_db."""
    clean_write = _clean_dsn(write_dsn)
    clean_read = _clean_dsn(read_dsn)
    if clean_write == clean_read:
        logger.info('WRITE_DB and READ_DB are identical; skipping table copy.')
        return 0

    with (
        psycopg.connect(clean_write) as w_conn,
        psycopg.connect(clean_read) as r_conn,
    ):
        tables = _get_copyable_tables(w_conn)
        logger.info(f'Found {len(tables)} tables to copy to read_db')

        with r_conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
            if tables:
                truncate_sql = (
                    'TRUNCATE TABLE '
                    + ', '.join(f'"{t}"' for t in tables)
                    + ' CASCADE;'
                )
                cur.execute(truncate_sql)

        copied = 0
        for table in tables:
            _copy_table_binary(w_conn, r_conn, table)
            copied += 1

        with r_conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'origin';")

        r_conn.commit()
        return copied


def refresh_materialized_views(read_dsn: str) -> tuple[int, int]:
    """Atualiza as 15 visões materializadas no banco de leitura."""
    logger.info('Refreshing materialized views on read_db')
    succeeded = 0
    failed = 0

    with psycopg.connect(_clean_dsn(read_dsn), autocommit=True) as conn:
        with conn.cursor() as cur:
            for mv in SEARCH_MATERIALIZED_VIEWS:
                try:
                    logger.debug(f'Refreshing materialized view {mv}')
                    cur.execute(
                        f'REFRESH MATERIALIZED VIEW CONCURRENTLY {mv};'
                    )
                    succeeded += 1
                except Exception as e:
                    logger.debug(
                        f'Concurrent refresh failed for {mv}, '
                        f'attempting standard refresh: {e}'
                    )
                    try:
                        cur.execute(f'REFRESH MATERIALIZED VIEW {mv};')
                        succeeded += 1
                    except Exception as err:
                        logger.warning(
                            f'Failed to refresh materialized view {mv}: {err}'
                        )
                        failed += 1

    return succeeded, failed


def bump_redis_cache(redis_url: str) -> bool:
    """Incrementa a geração de cache de busca no Redis."""
    try:
        r = redis.Redis.from_url(redis_url, socket_timeout=2.0)
        with r:
            gen = r.incr(REDIS_GENERATION_KEY)
            logger.info(f'Redis search cache generation bumped to {gen}')
            return True
    except Exception as e:
        logger.warning(f'Could not bump Redis search cache: {e}')
        return False


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Publica dados no banco de leitura, atualiza MVs e invalida cache."""
    logger.info('Starting publish_to_read_db routine')
    settings = Settings()

    write_dsn = settings.WRITE_DATABASE_URL
    read_dsn = settings.READ_DATABASE_URL

    copied_tables = copy_tables_to_read_db(write_dsn, read_dsn)
    mv_ok, mv_fail = refresh_materialized_views(read_dsn)

    if settings.REDIS_URL:
        bump_redis_cache(settings.REDIS_URL)

    total_items = copied_tables + len(SEARCH_MATERIALIZED_VIEWS)
    total_succeeded = copied_tables + mv_ok
    logger.info(
        f'Finished publish_to_read_db: {copied_tables} tables copied, '
        f'{mv_ok}/{len(SEARCH_MATERIALIZED_VIEWS)} MVs refreshed'
    )
    return Stats(
        found=total_items,
        succeeded=total_succeeded,
        failed=mv_fail,
    )
