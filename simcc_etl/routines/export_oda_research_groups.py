import asyncio
import os
from pathlib import Path
from typing import Any

import httpx
import polars as pl
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.export_oda_research_groups')

PAGE_SIZE = 500
CONCURRENCY = 10
TIMEOUT = httpx.Timeout(30.0)

SCHEMA = {
    'oda_id': pl.Utf8,
    'dgp_id': pl.Utf8,
    'name': pl.Utf8,
    'area': pl.Utf8,
    'year': pl.Int32,
    'situation': pl.Utf8,
    'institution_acronym': pl.Utf8,
    'institution_name': pl.Utf8,
    'leaders': pl.List(pl.Struct({'name': pl.Utf8, 'lattes_id': pl.Utf8})),
}


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
    return {
        'oda_id': str(group.get('id', '')),
        'dgp_id': group.get('dgpId'),
        'name': group.get('nome'),
        'area': group.get('areaPredominante'),
        'year': group.get('anoFormacao'),
        'situation': group.get('situacao'),
        'institution_acronym': inst.get('sigla'),
        'institution_name': inst.get('nome'),
        'leaders': leaders,
    }


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Sincroniza grupos de pesquisa da API ODA e salva em formato parquet."""
    logger.info('Starting export_oda_research_groups routine')
    settings = Settings()
    if not settings.ODA_API_URL:
        logger.warning('ODA_API_URL not configured. Skipping routine.')
        return Stats(found=0, succeeded=0, failed=0)

    async def _run_async() -> Stats:
        try:
            async with httpx.AsyncClient(
                base_url=settings.ODA_API_URL, timeout=TIMEOUT
            ) as client:
                groups = await _fetch_groups(client)
                found = len(groups)
                if not groups:
                    return Stats(found=0, succeeded=0, failed=0)

                semaphore = asyncio.Semaphore(CONCURRENCY)
                tasks = [
                    _fetch_leaders(client, semaphore, g['id']) for g in groups
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                records = []
                succeeded = 0
                failed = 0
                for g, res in zip(groups, results, strict=False):
                    if isinstance(res, Exception):
                        failed += 1
                        leaders = []
                    else:
                        succeeded += 1
                        leaders = res
                    records.append(_build_record(g, leaders))

                out_path = Path(settings.ODA_RESEARCH_GROUPS_PATH)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path = str(out_path) + '.tmp'
                pl.DataFrame(records, schema=SCHEMA).write_parquet(tmp_path)
                os.replace(tmp_path, out_path)
                logger.info(f'Exported {len(records)} groups to {out_path}')
                return Stats(found=found, succeeded=succeeded, failed=failed)
        except Exception as e:
            logger.warning(f'ODA API unreachable or returned error: {e}')
            return Stats(found=0, succeeded=0, failed=0)

    return asyncio.run(_run_async())
