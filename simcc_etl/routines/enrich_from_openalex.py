import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.enrich_from_openalex')

WORK_URL = 'https://api.openalex.org/works/https://doi.org/'
AUTHOR_URL = 'https://api.openalex.org/authors/orcid:'
DEFAULT_MAIL = 'suporte@simcc.org'


def _clean_orcid(orcid: str | None) -> str | None:
    if not orcid:
        return None
    cleaned = str(orcid).strip()
    for prefix in ('https://orcid.org/', 'http://orcid.org/', 'orcid.org/'):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
    return cleaned if cleaned else None


def _build_abstract(inverted_index: dict[str, list[int]] | None) -> str | None:
    if not inverted_index or not any(inverted_index.values()):
        return None
    length = max(max(v) for v in inverted_index.values())
    tokens = [''] * (length + 1)
    for word, positions in inverted_index.items():
        for pos in positions:
            tokens[pos] = word
    return ' '.join(tokens)


def _safe_get(data: Any, *keys: str) -> Any:
    for k in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(k)
    return data


def _partition_unseen_and_retry(
    rows: list[Any], not_found_path: Path
) -> list[Any]:
    """Ordena registros colocando os que já deram 404 no final da fila."""
    if not not_found_path.is_dir():
        return rows

    known_404: set[str] = {
        f.stem
        for f in not_found_path.iterdir()
        if f.is_file() and f.suffix == '.json'
    }
    if not known_404:
        return rows

    unseen = [row for row in rows if str(row['id']) not in known_404]
    retry = [row for row in rows if str(row['id']) in known_404]
    logger.info(
        f'Queue partitioned: {len(unseen)} unseen prioritized, '
        f'{len(retry)} prior 404 pushed to end'
    )
    return unseen + retry


def _get_or_fetch_payload(
    client: httpx.Client,
    url: str,
    cache_file: Path,
    not_found_file: Path | None = None,
) -> dict[str, Any] | None:
    if cache_file.is_file():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f'Failed to read local cache {cache_file}: {e}')

    try:
        resp = client.get(url)
        if resp.status_code == 200:
            payload = resp.json()
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(payload, f)
            if not_found_file and not_found_file.is_file():
                try:
                    not_found_file.unlink()
                except OSError:
                    pass
            return payload
        elif resp.status_code == 404 and not_found_file:
            try:
                with open(not_found_file, 'w', encoding='utf-8') as f:
                    json.dump(
                        {
                            'status_code': 404,
                            'attempted_at': (
                                datetime.now(timezone.utc).isoformat()
                            ),
                        },
                        f,
                    )
            except Exception as e:
                logger.warning(
                    f'Failed to write not_found cache {not_found_file}: {e}'
                )
    except Exception as e:
        logger.warning(f'Failed OpenAlex fetch for {url}: {e}')

    return None


def _process_article(
    client: httpx.Client,
    row: Any,
    base_storage: Path,
    session: Session,
    insert_art_sql: Any,
) -> bool:
    article_id = row['id']
    raw_doi = row['doi']
    doi = str(raw_doi).strip() if raw_doi else ''
    if not doi:
        return False

    art_file = base_storage / 'article' / f'{article_id}.json'
    not_found_file = (
        base_storage / 'not_found' / 'article' / f'{article_id}.json'
    )
    url = f'{WORK_URL}{doi}?mailto={DEFAULT_MAIL}'

    payload = _get_or_fetch_payload(client, url, art_file, not_found_file)
    if payload is None:
        return False

    try:
        abstract = _build_abstract(payload.get('abstract_inverted_index'))
        authorships = payload.get('authorships', [])
        authors = [
            _safe_get(a, 'author', 'display_name')
            for a in authorships
            if _safe_get(a, 'author', 'display_name')
        ]
        inst_name = _safe_get(
            payload, 'primary_location', 'source', 'display_name'
        )
        issn_val = _safe_get(payload, 'primary_location', 'source', 'issn')
        issn_str = str(issn_val) if issn_val is not None else None

        with session.begin_nested():
            session.execute(
                insert_art_sql,
                {
                    'id': uuid4(),
                    'article_id': article_id,
                    'article_institution': inst_name,
                    'issn': issn_str,
                    'abstract': abstract,
                    'authors': '; '.join(authors),
                    'citations_count': payload.get('cited_by_count', 0),
                    'language': payload.get('language'),
                    'landing_page_url': _safe_get(
                        payload, 'primary_location', 'landing_page_url'
                    ),
                    'pdf': _safe_get(payload, 'primary_location', 'pdf_url'),
                },
            )
        return True
    except Exception as e:
        logger.warning(
            f'Failed to insert article {article_id} (DOI: {doi}): {e}'
        )
        return False


def _process_researcher(
    client: httpx.Client,
    row: Any,
    base_storage: Path,
    session: Session,
    insert_res_sql: Any,
) -> bool:
    r_id = row['id']
    raw_orcid = row['orcid']
    orcid = _clean_orcid(raw_orcid)
    if not orcid:
        return False

    res_file = base_storage / 'researcher' / f'{r_id}.json'
    not_found_file = (
        base_storage / 'not_found' / 'researcher' / f'{r_id}.json'
    )
    url = f'{AUTHOR_URL}{orcid}?mailto={DEFAULT_MAIL}'

    payload = _get_or_fetch_payload(client, url, res_file, not_found_file)
    if payload is None:
        return False

    try:
        with session.begin_nested():
            session.execute(
                insert_res_sql,
                {
                    'researcher_id': r_id,
                    'h_index': _safe_get(payload, 'summary_stats', 'h_index'),
                    'works_count': payload.get('works_count'),
                    'cited_by_count': payload.get('cited_by_count'),
                    'i10_index': _safe_get(
                        payload, 'summary_stats', 'i10_index'
                    ),
                    'scopus': _safe_get(payload, 'ids', 'scopus'),
                    'orcid': orcid[:19],
                    'openalex': payload.get('id'),
                },
            )
        return True
    except Exception as e:
        logger.warning(
            f'Failed to insert researcher {r_id} (ORCID: {orcid}): {e}'
        )
        return False


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    batch_size: int | None = None,
    storage_dir: Path | None = None,
) -> Stats:
    """Consulta OpenAlex por novos artigos (DOI) e pesquisadores (ORCID)."""
    logger.info('Starting enrich_from_openalex routine')
    settings = Settings()
    effective_batch_size = batch_size or settings.OPENALEX_BATCH_COMMIT_SIZE

    base_storage = storage_dir or (settings.STORAGE_DIR / 'openalex')
    art_path = base_storage / 'article'
    res_path = base_storage / 'researcher'
    art_not_found_path = base_storage / 'not_found' / 'article'
    res_not_found_path = base_storage / 'not_found' / 'researcher'
    art_path.mkdir(parents=True, exist_ok=True)
    res_path.mkdir(parents=True, exist_ok=True)
    art_not_found_path.mkdir(parents=True, exist_ok=True)
    res_not_found_path.mkdir(parents=True, exist_ok=True)

    headers = {'User-Agent': 'simcc-etl/1.0'}
    if settings.OPENALEX_API_KEY:
        headers['api-key'] = settings.OPENALEX_API_KEY

    # 1. Artigos
    art_where = [
        "bp.doi IS NOT NULL",
        "TRIM(bp.doi) != ''",
        "oa.article_id IS NULL",
    ]
    art_params: dict[str, Any] = {}
    if researcher_ids:
        art_where.append("bp.researcher_id = ANY(:researcher_ids)")
        art_params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        art_where.append("r.lattes_id = ANY(:lattes_ids)")
        art_params['lattes_ids'] = list(lattes_ids)

    art_join_researcher = (
        "JOIN researcher r ON r.id = bp.researcher_id" if lattes_ids else ""
    )
    art_sql_str = f"""
        SELECT bp.id, bp.doi
        FROM bibliographic_production bp
        {art_join_researcher}
        LEFT JOIN openalex_article oa ON oa.article_id = bp.id
        WHERE {' AND '.join(art_where)};
    """
    art_rows = session.execute(text(art_sql_str), art_params).mappings().all()

    # 2. Pesquisadores
    res_where = [
        "r.orcid IS NOT NULL",
        "TRIM(r.orcid) != ''",
        "oar.researcher_id IS NULL",
    ]
    res_params: dict[str, Any] = {}
    if researcher_ids:
        res_where.append("r.id = ANY(:researcher_ids)")
        res_params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        res_where.append("r.lattes_id = ANY(:lattes_ids)")
        res_params['lattes_ids'] = list(lattes_ids)

    res_sql_str = f"""
        SELECT r.id, r.orcid
        FROM researcher r
        LEFT JOIN openalex_researcher oar ON oar.researcher_id = r.id
        WHERE {' AND '.join(res_where)};
    """
    res_rows = session.execute(text(res_sql_str), res_params).mappings().all()

    # Particionamento: inéditos no topo, 404 no fim da fila
    art_rows = _partition_unseen_and_retry(art_rows, art_not_found_path)
    res_rows = _partition_unseen_and_retry(res_rows, res_not_found_path)

    total_found = len(art_rows) + len(res_rows)
    succeeded = 0
    failed = 0
    uncommitted_count = 0

    insert_art_sql = text("""
        INSERT INTO openalex_article (
            id, article_id, article_institution, issn,
            abstract, authors, citations_count, language,
            landing_page_url, pdf
        ) VALUES (
            :id, :article_id, :article_institution, :issn,
            :abstract, :authors, :citations_count, :language,
            :landing_page_url, :pdf
        )
        ON CONFLICT (article_id) DO NOTHING;
    """)

    insert_res_sql = text("""
        INSERT INTO openalex_researcher (
            researcher_id, h_index, works_count,
            cited_by_count, i10_index, scopus, orcid, openalex
        ) VALUES (
            :researcher_id, :h_index, :works_count,
            :cited_by_count, :i10_index, :scopus,
            :orcid, :openalex
        )
        ON CONFLICT (researcher_id) DO NOTHING;
    """)

    with httpx.Client(headers=headers, timeout=15.0) as client:
        # Process articles
        for row in art_rows:
            if _process_article(
                client, row, base_storage, session, insert_art_sql
            ):
                succeeded += 1
                uncommitted_count += 1
                if uncommitted_count >= effective_batch_size:
                    session.commit()
                    logger.debug(
                        f'Committed batch of {uncommitted_count} '
                        'records to database'
                    )
                    uncommitted_count = 0
            else:
                failed += 1

        # Process researchers
        for row in res_rows:
            if _process_researcher(
                client, row, base_storage, session, insert_res_sql
            ):
                succeeded += 1
                uncommitted_count += 1
                if uncommitted_count >= effective_batch_size:
                    session.commit()
                    logger.debug(
                        f'Committed batch of {uncommitted_count} '
                        'records to database'
                    )
                    uncommitted_count = 0
            else:
                failed += 1

    # Commit residual para quaisquer registros restantes
    if uncommitted_count > 0:
        session.commit()
        logger.debug(
            f'Committed residual batch of {uncommitted_count} '
            'records to database'
        )
        uncommitted_count = 0

    logger.info(
        f'Finished enrich_from_openalex: {succeeded}/{total_found} '
        f'succeeded, {failed} failed'
    )
    return Stats(found=total_found, succeeded=succeeded, failed=failed)
