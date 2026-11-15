import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.export_oda_research_groups')

PAGE_SIZE = 500
CONCURRENCY = 10
TIMEOUT = httpx.Timeout(30.0)


def should_run_sync(session: Session, interval_days: int = 7) -> bool:
    """Verifica se passaram pelo menos interval_days desde a última execução com sucesso."""
    try:
        stmt = text("""
            SELECT started_at FROM public.etl_runs
            WHERE (
                (steps->'export_oda_research_groups'->>'status') = 'SUCCEEDED'
                OR (current_routine = 'export_oda_research_groups' AND status = 'SUCCEEDED')
            )
            ORDER BY started_at DESC
            LIMIT 1;
        """)
        last_run = session.execute(stmt).scalar()
        if not last_run:
            return True
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        elapsed = datetime.now(timezone.utc) - last_run
        return elapsed >= timedelta(days=interval_days)
    except Exception as e:
        logger.warning(
            f'Erro ao verificar histórico de etl_runs: {e}. '
            'Executando sincronização por precaução.'
        )
        return True


async def _fetch_groups(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    page = 1
    while True:
        resp = await client.get(
            '/grupos-pesquisa/simcc', params={'page': page, 'size': PAGE_SIZE}
        )
        resp.raise_for_status()
        data = resp.json()
        groups.extend(data.get('data', []))
        total_pages = data.get('meta', {}).get('totalPages', 1)
        if page >= total_pages:
            break
        page += 1
    return groups


async def _fetch_leaders(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, group_id: str
) -> list[dict[str, str]]:
    async with semaphore:
        resp = await client.get(
            f'/grupos-pesquisa/{group_id}/pesquisadores',
            params={'eLider': 'true', 'size': 30},
        )
        resp.raise_for_status()
        data = resp.json()
    return [
        {'name': m.get('nome', ''), 'lattes_id': m.get('lattesId', '')}
        for m in data.get('data', [])
    ]


def _build_record(
    group: dict[str, Any], leaders: list[dict[str, str]]
) -> dict[str, Any]:
    institutions = group.get('instituicoes') or []
    inst = next(
        (i for i in institutions if i.get('tipoRelacao') == 'SEDE'), None
    ) or (institutions[0] if institutions else {})
    oda_id = str(group.get('id', ''))
    dgp_id = group.get('dgpId') or f'ODA_{oda_id}'
    return {
        'oda_id': oda_id,
        'dgp_id': dgp_id,
        'name': group.get('nome'),
        'area': group.get('areaPredominante'),
        'year': group.get('anoFormacao'),
        'situation': group.get('situacao'),
        'institution_acronym': inst.get('sigla'),
        'institution_name': inst.get('nome'),
        'leaders': leaders,
    }


def _get_researcher_lattes_map(session: Session) -> dict[str, str]:
    """Retorna dicionário mapeando lattes_id para id do pesquisador."""
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT, lattes_id FROM public.researcher WHERE lattes_id IS NOT NULL;'
            )
        )
        .mappings()
        .all()
    )
    return {r['lattes_id'].strip(): r['id'] for r in rows if r['lattes_id']}


def _save_groups_to_database(
    session: Session, records: list[dict[str, Any]]
) -> tuple[int, int]:
    """Salva os grupos e seus líderes nas tabelas research_group e research_group_researcher."""
    researcher_map = _get_researcher_lattes_map(session)

    upsert_group_sql = text("""
        INSERT INTO public.research_group (
            name, institution, first_leader, first_leader_id,
            second_leader, second_leader_id, area, group_identifier,
            year, institution_name
        ) VALUES (
            :name, :institution, :first_leader, :first_leader_id,
            :second_leader, :second_leader_id, :area, :group_identifier,
            :year, :institution_name
        )
        ON CONFLICT (group_identifier) DO UPDATE SET
            name = EXCLUDED.name,
            institution = EXCLUDED.institution,
            first_leader = EXCLUDED.first_leader,
            first_leader_id = EXCLUDED.first_leader_id,
            second_leader = EXCLUDED.second_leader,
            second_leader_id = EXCLUDED.second_leader_id,
            area = EXCLUDED.area,
            year = EXCLUDED.year,
            institution_name = EXCLUDED.institution_name
        RETURNING id, group_identifier;
    """)

    insert_relation_sql = text("""
        INSERT INTO public.research_group_researcher (research_group_id, researcher_id)
        VALUES (:research_group_id, :researcher_id)
        ON CONFLICT (research_group_id, researcher_id) DO NOTHING;
    """)

    succeeded = 0
    failed = 0

    for rec in records:
        try:
            leaders = (rec.get('leaders') or [])[:2]
            leaders += [{'name': None, 'lattes_id': None}] * (2 - len(leaders))
            first, second = leaders

            first_lattes = (first.get('lattes_id') or '').strip()
            second_lattes = (second.get('lattes_id') or '').strip()

            first_id = researcher_map.get(first_lattes) if first_lattes else None
            second_id = (
                researcher_map.get(second_lattes) if second_lattes else None
            )

            params = {
                'name': rec.get('name'),
                'institution': rec.get('institution_acronym'),
                'first_leader': first.get('name'),
                'first_leader_id': first_id,
                'second_leader': second.get('name'),
                'second_leader_id': second_id,
                'area': rec.get('area'),
                'group_identifier': rec.get('dgp_id'),
                'year': rec.get('year'),
                'institution_name': rec.get('institution_name'),
            }

            res = session.execute(upsert_group_sql, params).mappings().first()
            if res and res.get('id'):
                group_id = res['id']
                relations = []
                if first_id:
                    relations.append({
                        'research_group_id': group_id,
                        'researcher_id': first_id,
                    })
                if second_id and second_id != first_id:
                    relations.append({
                        'research_group_id': group_id,
                        'researcher_id': second_id,
                    })
                if relations:
                    session.execute(insert_relation_sql, relations)

            succeeded += 1
        except Exception as e:
            logger.error(
                f"Erro ao salvar grupo {rec.get('name')}: {e}", exc_info=True
            )
            failed += 1

    session.commit()
    return succeeded, failed


def run(
    session: Session,
    *,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    force: bool = False,
) -> Stats:
    """Sincroniza grupos de pesquisa da API ODA e registra no banco relacional.

    Verifica se já se passaram 7 dias desde a última execução bem-sucedida.
    """
    logger.info('Starting export_oda_research_groups routine')
    settings = Settings()

    if not force and not should_run_sync(
        session, settings.ODA_SYNC_INTERVAL_DAYS
    ):
        logger.info(
            f'Grupos de pesquisa da ODA sincronizados recentemente '
            f'(< {settings.ODA_SYNC_INTERVAL_DAYS} dias). Pulando rotina.'
        )
        return Stats(found=0, succeeded=0, failed=0)

    if not settings.ODA_API_URL:
        logger.warning('ODA_API_URL not configured. Skipping routine.')
        return Stats(found=0, succeeded=0, failed=0)

    async def _run_async() -> Stats:
        async with httpx.AsyncClient(
            base_url=settings.ODA_API_URL, timeout=TIMEOUT
        ) as client:
            groups = await _fetch_groups(client)
            found = len(groups)
            if not groups:
                logger.info('Nenhum grupo retornado pela ODA.')
                return Stats(found=0, succeeded=0, failed=0)

            semaphore = asyncio.Semaphore(CONCURRENCY)
            tasks = [
                _fetch_leaders(client, semaphore, g['id']) for g in groups
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            records = []
            for g, res in zip(groups, results, strict=False):
                leaders = [] if isinstance(res, Exception) else res
                records.append(_build_record(g, leaders))

            succeeded, failed = _save_groups_to_database(session, records)
            logger.info(
                f'Sincronização ODA concluída: {succeeded} grupos salvos no banco, {failed} falhas.'
            )
            return Stats(found=found, succeeded=succeeded, failed=failed)

    return asyncio.run(_run_async())
