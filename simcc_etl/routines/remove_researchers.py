import io

import httpx
import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import clean_lattes_id

logger = get_logger('simcc_etl.routines.remove_researchers')

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

_NESTED_CLEANUP_TARGETS: tuple[tuple[str, str, str], ...] = (
    (
        'bibliographic_production_article',
        'bibliographic_production_id',
        'bibliographic_production',
    ),
    (
        'bibliographic_production_book',
        'bibliographic_production_id',
        'bibliographic_production',
    ),
    (
        'bibliographic_production_book_chapter',
        'bibliographic_production_id',
        'bibliographic_production',
    ),
    (
        'bibliographic_production_work_in_event',
        'bibliographic_production_id',
        'bibliographic_production',
    ),
    ('process_author', 'process_id', 'process_or_technique'),
    ('process_keyword', 'process_id', 'process_or_technique'),
    ('process_knowledge_area', 'process_id', 'process_or_technique'),
    ('research_project_components', 'project_id', 'research_project'),
    ('research_project_foment', 'project_id', 'research_project'),
    ('research_project_production', 'project_id', 'research_project'),
)


def _fetch_active_lattes_ids(settings: Settings) -> list[str]:
    """Obtém a lista de lattes_id ativos no simcc-admin via HTTP Parquet."""
    url = f'{settings.ADMIN_API_URL}/academic/researchers/export/parquet'
    headers = {'X-Service-Token': settings.ADMIN_SERVICE_TOKEN}
    transport = httpx.HTTPTransport(retries=3)

    try:
        with httpx.Client(transport=transport, timeout=TIMEOUT) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
    except Exception as e:
        logger.error(f'Falha ao consultar pesquisadores no simcc-admin: {e}')
        raise RuntimeError(
            f'Erro ao obter pesquisadores do simcc-admin: {e}'
        ) from e

    df = pl.read_parquet(io.BytesIO(resp.content))
    if 'lattes_id' not in df.columns:
        raise ValueError(
            "Coluna 'lattes_id' ausente no Parquet retornado pelo simcc-admin."
        )

    active: list[str] = []
    seen: set[str] = set()
    for row in df.iter_rows(named=True):
        lid = clean_lattes_id(row.get('lattes_id'))
        if lid and lid not in seen:
            seen.add(lid)
            active.append(lid)

    if not active:
        logger.error(
            'Nenhum pesquisador com Lattes ID válido retornado '
            'pelo simcc-admin.'
        )
        raise RuntimeError(
            'A lista de pesquisadores retornada pelo simcc-admin está vazia. '
            'Abortando remoção de pesquisadores para evitar exclusão em massa.'
        )

    return active


def _find_target_researcher_ids(
    session: Session,
    *,
    researcher_ids: list[str] | None,
    lattes_ids: list[str] | None,
    settings: Settings,
) -> list[str]:
    """Identifica os IDs em public.researcher que devem ser removidos."""
    if researcher_ids or lattes_ids:
        clauses: list[str] = []
        params: dict[str, object] = {}

        if researcher_ids:
            clauses.append('id::TEXT = ANY(:researcher_ids)')
            params['researcher_ids'] = [str(rid) for rid in researcher_ids]

        if lattes_ids:
            cleaned = [
                clean_lattes_id(lid)
                for lid in lattes_ids
                if clean_lattes_id(lid)
            ]
            if cleaned:
                clauses.append('lattes_id = ANY(:lattes_ids)')
                params['lattes_ids'] = list(dict.fromkeys(cleaned))

        if not clauses:
            return []

        where_sql = ' OR '.join(clauses)
        rows = session.execute(
            text(f'SELECT id::TEXT FROM public.researcher WHERE {where_sql};'),
            params,
        ).fetchall()
        return [str(r[0]) for r in rows]

    active_lattes_ids = _fetch_active_lattes_ids(settings)
    rows = session.execute(
        text(
            'SELECT id::TEXT FROM public.researcher '
            'WHERE lattes_id IS NULL '
            'OR NOT (lattes_id = ANY(:active_lattes_ids));'
        ),
        {'active_lattes_ids': active_lattes_ids},
    ).fetchall()
    return [str(r[0]) for r in rows]


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
) -> Stats:
    """Remove pesquisadores excluídos do simcc-admin (ou informados)."""
    if settings is None:
        settings = Settings()

    target_ids = _find_target_researcher_ids(
        session,
        researcher_ids=researcher_ids,
        lattes_ids=lattes_ids,
        settings=settings,
    )
    if not target_ids:
        logger.info('Nenhum pesquisador para remover em public.researcher.')
        return Stats(found=0, succeeded=0, failed=0)

    logger.info(
        f'Removendo {len(target_ids)} pesquisador(es) de public.researcher'
    )
    session.execute(
        text(
            'UPDATE public.research_group '
            'SET first_leader_id = NULL '
            'WHERE first_leader_id::TEXT = ANY(:ids);'
        ),
        {'ids': target_ids},
    )
    session.execute(
        text(
            'UPDATE public.research_group '
            'SET second_leader_id = NULL '
            'WHERE second_leader_id::TEXT = ANY(:ids);'
        ),
        {'ids': target_ids},
    )
    session.execute(
        text(
            'UPDATE public.labs '
            'SET researcher_id = NULL '
            'WHERE researcher_id::TEXT = ANY(:ids);'
        ),
        {'ids': target_ids},
    )
    for child_table, fk_col, parent_table in _NESTED_CLEANUP_TARGETS:
        session.execute(
            text(
                f'DELETE FROM public.{child_table} '
                f'WHERE {fk_col} IN ('
                f'SELECT id FROM public.{parent_table} '
                'WHERE researcher_id::TEXT = ANY(:ids)'
                ');'
            ),
            {'ids': target_ids},
        )
    session.execute(
        text('DELETE FROM public.researcher WHERE id::TEXT = ANY(:ids);'),
        {'ids': target_ids},
    )
    session.commit()

    return Stats(found=len(target_ids), succeeded=len(target_ids), failed=0)
