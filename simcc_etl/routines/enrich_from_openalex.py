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


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Consulta OpenAlex por novos artigos (DOI) e pesquisadores (ORCID)."""
    logger.info('Starting enrich_from_openalex routine')
    settings = Settings()
    base_storage = Path('storage/openalex')
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

    with httpx.Client(headers=headers, timeout=15.0) as client:
        # Process articles
        for row in art_rows:
            article_id = row['id']
            doi = row['doi']
            try:
                url = f'{WORK_URL}{doi}?mailto={DEFAULT_MAIL}'
                resp = client.get(url)
                if resp.status_code == 200:
                    payload = resp.json()
                    abstract = _build_abstract(
                        payload.get('abstract_inverted_index')
                    )
                    authorships = payload.get('authorships', [])
                    authors = [
                        _safe_get(a, 'author', 'display_name')
                        for a in authorships
                        if _safe_get(a, 'author', 'display_name')
                    ]

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
                    inst_name = _safe_get(
                        payload, 'primary_location', 'source', 'display_name'
                    )
                    issn_val = _safe_get(
                        payload, 'primary_location', 'source', 'issn'
                    )
                    issn_str = str(issn_val)
                    session.execute(
                        insert_art_sql,
                        {
                            'id': uuid4(),
                            'article_id': article_id,
                            'article_institution': inst_name,
                            'issn': issn_str,
                            'abstract': abstract,
                            'authors': '; '.join(authors),
                            'citations_count': payload.get(
                                'cited_by_count', 0
                            ),
                            'language': payload.get('language'),
                            'landing_page_url': _safe_get(
                                payload, 'primary_location', 'landing_page_url'
                            ),
                            'pdf': _safe_get(
                                payload, 'primary_location', 'pdf_url'
                            ),
                        },
                    )
                    with open(
                        art_path / f'{article_id}.json', 'w', encoding='utf-8'
                    ) as f:
                        json.dump(payload, f)
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f'Failed OpenAlex fetch for article {doi}: {e}')
                failed += 1

        # Process researchers
        for row in res_rows:
            r_id = row['id']
            orcid = row['orcid']
            try:
                url = f'{AUTHOR_URL}{orcid}?mailto={DEFAULT_MAIL}'
                resp = client.get(url)
                if resp.status_code == 200:
                    payload = resp.json()
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
                    session.execute(
                        insert_res_sql,
                        {
                            'researcher_id': r_id,
                            'h_index': _safe_get(
                                payload, 'summary_stats', 'h_index'
                            ),
                            'works_count': payload.get('works_count'),
                            'cited_by_count': payload.get('cited_by_count'),
                            'i10_index': _safe_get(
                                payload, 'summary_stats', 'i10_index'
                            ),
                            'scopus': _safe_get(payload, 'ids', 'scopus'),
                            'orcid': orcid,
                            'openalex': payload.get('id'),
                        },
                    )
                    with open(
                        res_path / f'{r_id}.json', 'w', encoding='utf-8'
                    ) as f:
                        json.dump(payload, f)
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(
                    f'Failed OpenAlex fetch for researcher {orcid}: {e}'
                )
                failed += 1

    session.commit()
    logger.info(
        f'Finished enrich_from_openalex: {succeeded}/{total_found} '
        f'succeeded, {failed} failed'
    )
    return Stats(found=total_found, succeeded=succeeded, failed=failed)
