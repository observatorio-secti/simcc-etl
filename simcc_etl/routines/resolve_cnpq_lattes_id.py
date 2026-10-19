import re
import ssl
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger

logger = get_logger('simcc_etl.routines.resolve_cnpq_lattes_id')

LATTES_10_PATTERN = re.compile(r'^[A-Za-z0-9]{10}$')
LATTES_10_LEN = 10


def _create_legacy_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    return context


def fetch_lattes_10_id(lattes_id: str) -> str | None:
    """Tenta obter o identificador Lattes de 10 chars via redirect."""
    if not lattes_id:
        return None

    url = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
        )
    }

    try:
        ssl_ctx = _create_legacy_ssl_context()
        with httpx.Client(
            follow_redirects=False, headers=headers, verify=ssl_ctx
        ) as client:
            resp = client.get(url, timeout=15.0)
            if resp.status_code == HTTPStatus.FOUND:
                location = resp.headers.get('Location', '')
                if not location or 'erro.jsp' in location.lower():
                    return None

                parsed = urlparse(location)
                query_params = parse_qs(parsed.query)
                candidate = query_params.get('id', [None])[0]
                if candidate and LATTES_10_PATTERN.match(candidate):
                    return candidate

                if len(location) >= LATTES_10_LEN:
                    last_10 = location[-LATTES_10_LEN:]
                    if LATTES_10_PATTERN.match(last_10):
                        return last_10
    except Exception as e:
        logger.warning(f'Error requesting lattes_10 for {lattes_id}: {e}')
        return None

    return None


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Resolve e persiste o ID de 10 dígitos do CNPq para os pesquisadores."""
    logger.info('Starting resolve_cnpq_lattes_id routine')
    where_clauses = ['1=1']
    params: dict[str, object] = {}

    if researcher_ids or lattes_ids:
        if researcher_ids:
            where_clauses.append('id = ANY(:researcher_ids)')
            params['researcher_ids'] = list(researcher_ids)
        if lattes_ids:
            where_clauses.append('lattes_id = ANY(:lattes_ids)')
            params['lattes_ids'] = list(lattes_ids)
    else:
        where_clauses.append('lattes_10_id IS NULL')

    where_sql = ' AND '.join(where_clauses)
    sql = text(f'SELECT id, lattes_id FROM researcher WHERE {where_sql};')
    researchers = session.execute(sql, params).mappings().all()

    found = len(researchers)
    succeeded = 0
    failed = 0

    update_sql = text("""
        UPDATE researcher
        SET lattes_10_id = :lattes_10_id
        WHERE id = :id;
    """)

    for row in researchers:
        r_id = row['id']
        l_id = row['lattes_id']
        try:
            l10 = fetch_lattes_10_id(l_id)
            if l10:
                session.execute(update_sql, {'id': r_id, 'lattes_10_id': l10})
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            logger.warning(f'Failed resolving lattes_10 for {r_id}: {e}')
            failed += 1

    session.commit()
    logger.info(
        f'Finished resolve_cnpq_lattes_id: {succeeded}/{found} resolved, '
        f'{failed} unresolved'
    )
    return Stats(found=found, succeeded=succeeded, failed=failed)
