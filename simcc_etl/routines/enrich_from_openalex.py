import json
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


def _get_or_fetch_payload(
    client: httpx.Client,
    url: str,
    cache_file: Path,
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
            return payload
    except Exception as e:
        logger.warning(f'Failed OpenAlex fetch for {url}: {e}')

    return None


def _process_article(
    client: httpx.Client,
    row: Any,
    art_path: Path,
    session: Session,
    insert_art_sql: Any,
) -> bool:
    article_id = row['id']
    doi = row['doi']
    art_file = art_path / f'{article_id}.json'
    url = f'{WORK_URL}{doi}?mailto={DEFAULT_MAIL}'

    payload = _get_or_fetch_payload(client, url, art_file)
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
        session.rollback()
        return False


def _process_researcher(
    client: httpx.Client,
    row: Any,
    res_path: Path,
    session: Session,
    insert_res_sql: Any,
) -> bool:
    r_id = row['id']
    orcid = row['orcid']
    res_file = res_path / f'{r_id}.json'
    url = f'{AUTHOR_URL}{orcid}?mailto={DEFAULT_MAIL}'

    payload = _get_or_fetch_payload(client, url, res_file)
    if payload is None:
        return False

    try:
        session.execute(
            insert_res_sql,
            {
                'researcher_id': r_id,
                'h_index': _safe_get(payload, 'summary_stats', 'h_index'),
                'works_count': payload.get('works_count'),
                'cited_by_count': payload.get('cited_by_count'),
                'i10_index': _safe_get(payload, 'summary_stats', 'i10_index'),
                'scopus': _safe_get(payload, 'ids', 'scopus'),
                'orcid': orcid,
                'openalex': payload.get('id'),
            },
        )
        return True
    except Exception as e:
        logger.warning(
            f'Failed to insert researcher {r_id} (ORCID: {orcid}): {e}'
        )
        session.rollback()
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
    art_path.mkdir(parents=True, exist_ok=True)
    res_path.mkdir(parents=True, exist_ok=True)

    headers = {'User-Agent': 'simcc-etl/1.0'}
    if settings.OPENALEX_API_KEY:
        headers['api-key'] = settings.OPENALEX_API_KEY

    # 1. Artigos
    art_sql = text("""
        SELECT bp.id, bp.doi
        FROM bibliographic_production bp
        LEFT JOIN openalex_article oa ON oa.article_id = bp.id
        WHERE bp.doi IS NOT NULL AND oa.article_id IS NULL;
    """)
    art_rows = session.execute(art_sql).mappings().all()

    # 2. Pesquisadores
    res_sql = text("""
        SELECT r.id, r.orcid
        FROM researcher r
        LEFT JOIN openalex_researcher oar ON oar.researcher_id = r.id
        WHERE r.orcid IS NOT NULL AND oar.researcher_id IS NULL;
    """)
    res_rows = session.execute(res_sql).mappings().all()

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
                client, row, art_path, session, insert_art_sql
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
                uncommitted_count = 0
                failed += 1

        # Process researchers
        for row in res_rows:
            if _process_researcher(
                client, row, res_path, session, insert_res_sql
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
                uncommitted_count = 0
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
