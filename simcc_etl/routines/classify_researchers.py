import datetime
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger

logger = get_logger('simcc_etl.routines.classify_researchers')

YEAR_FILTER = 2019
BATCH_SIZE = 5000

QUALIS_COLUMNS = [
    'researcher_id',
    'A1',
    'A2',
    'A3',
    'A4',
    'B1',
    'B2',
    'B3',
    'B4',
    'C',
    'SQ',
]

GUIDANCE_RENAME = {
    'iniciacao cientifica concluida': 'ic_completed',
    'iniciacao cientifica em andamento': 'ic_in_progress',
    'dissertacao de mestrado concluida': 'm_completed',
    'dissertacao de mestrado em andamento': 'm_in_progress',
    'tese de doutorado concluida': 'd_completed',
    'tese de doutorado em andamento': 'd_in_progress',
    'trabalho de conclusão de curso graduacao concluida': ('g_completed'),
    'trabalho de conclusao de curso de graduacao em andamento': (
        'g_in_progress'
    ),
    (
        'monografia de conclusao de curso aperfeicoamento e especializacao '
        'concluida'
    ): 'e_completed',
    (
        'monografia de conclusao de curso aperfeicoamento e especializacao '
        'em andamento'
    ): 'e_in_progress',
    'orientacao-de-outra-natureza concluida': 'o_completed',
    'supervisao de pos-doutorado concluida': 'sd_completed',
    'supervisao de pos-doutorado em andamento': 'sd_in_progress',
}


def _article_metrics(session: Session, year: int) -> pl.DataFrame:
    sql = text("""
        SELECT bpa.qualis, COUNT(*) AS count_article, bp.researcher_id::TEXT
        FROM public.bibliographic_production bp
        JOIN public.bibliographic_production_article bpa
          ON bp.id = bpa.bibliographic_production_id
        WHERE bp.type = 'ARTICLE' AND bp.year_ >= :year
        GROUP BY bpa.qualis, bp.researcher_id;
    """)
    rows = session.execute(sql, {'year': year}).mappings().all()
    schema = {
        c: pl.Int64 if c != 'researcher_id' else pl.String
        for c in QUALIS_COLUMNS
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )
    df = df.pivot(
        on='qualis',
        index='researcher_id',
        values='count_article',
        aggregate_function='sum',
    ).fill_null(0)

    for col in QUALIS_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    return df.select(QUALIS_COLUMNS)


def _patent_metrics(session: Session, year: int) -> pl.DataFrame:
    sql = text("""
        SELECT researcher_id::TEXT,
               COUNT(*) FILTER (
                   WHERE p.grant_date IS NULL
               ) AS patent_not_granted,
               COUNT(*) FILTER (
                   WHERE p.grant_date IS NOT NULL
               ) AS patent_granted
        FROM patent p
        WHERE development_year::INT >= :year
        GROUP BY researcher_id;
    """)
    rows = session.execute(sql, {'year': year}).mappings().all()
    cols = ['researcher_id', 'patent_not_granted', 'patent_granted']
    if not rows:
        return pl.DataFrame(
            schema={
                c: pl.Int64 if c != 'researcher_id' else pl.String
                for c in cols
            }
        )
    return pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )


def _guidance_metrics(session: Session, year: int) -> pl.DataFrame:
    sql = text("""
        SELECT researcher_id::TEXT,
               unaccent(lower((g.nature || ' ' || g.status))) AS nature,
               COUNT(*) AS count_nature
        FROM guidance g
        WHERE g.year >= :year
        GROUP BY nature, g.status, g.researcher_id;
    """)
    rows = session.execute(sql, {'year': year}).mappings().all()
    cols = ['researcher_id'] + list(GUIDANCE_RENAME.values())
    if not rows:
        return pl.DataFrame(
            schema={
                c: pl.Int64 if c != 'researcher_id' else pl.String
                for c in cols
            }
        )

    df = pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )
    df = df.pivot(
        on='nature',
        index='researcher_id',
        values='count_nature',
        aggregate_function='sum',
    ).fill_null(0)

    rename_map = {k: v for k, v in GUIDANCE_RENAME.items() if k in df.columns}
    df = df.rename(rename_map)

    for col in cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    return df.select(cols)


def _education_metrics(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT researcher_id::TEXT, MAX(education_end) AS first_doc
        FROM education
        WHERE degree = 'DOCTORATE'
        GROUP BY researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    if not rows:
        return pl.DataFrame(
            schema={'researcher_id': pl.String, 'first_doc': pl.Int64}
        )
    return pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String),
        pl.col('first_doc').cast(pl.Int64),
    )


def _simple_count_metrics(
    session: Session, sql: str, params: dict[str, Any], column_name: str
) -> pl.DataFrame:
    rows = session.execute(text(sql), params).mappings().all()
    if not rows:
        return pl.DataFrame(
            schema={'researcher_id': pl.String, column_name: pl.Int64}
        )
    return pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )


def calculate_rank(row: dict[str, Any]) -> str:
    first_doc = row.get('first_doc') or 0
    if first_doc == 0:
        return 'E'

    years_doc = datetime.datetime.now().year - first_doc
    qualis_a = (
        row.get('A1', 0)
        + row.get('A2', 0)
        + row.get('A3', 0)
        + row.get('A4', 0)
    )
    qualis_b = (
        row.get('B1', 0)
        + row.get('B2', 0)
        + row.get('B3', 0)
        + row.get('B4', 0)
    )
    qualis_na = row.get('C', 0) + row.get('SQ', 0)
    articles = qualis_a + qualis_b + qualis_na
    academic_prod = articles + row.get('book', 0) + row.get('book_chapter', 0)

    doc_guidance = row.get('d_completed', 0) + row.get('d_in_progress', 0)
    masters_guidance = row.get('m_completed', 0) + row.get('m_in_progress', 0)
    patent = row.get('patent_granted', 0) + row.get('patent_not_granted', 0)
    software = row.get('software', 0)
    a1 = row.get('A1', 0)

    if years_doc >= 10:
        if (academic_prod >= 5 and a1 >= 2 and doc_guidance >= 4) or (
            a1 >= 1 and patent >= 1
        ):
            return 'A+'
        if (academic_prod >= 5 and a1 >= 1 and doc_guidance >= 2) or (
            patent >= 1
        ):
            return 'A'

    if years_doc >= 8:
        guidance_cond = masters_guidance >= 2 or doc_guidance >= 1
        tech_cond = patent >= 1 or software >= 3
        if (academic_prod >= 4 and qualis_a >= 2 and guidance_cond) or (
            qualis_a >= 1 and tech_cond
        ):
            return 'B+'
        if (
            academic_prod >= 4 and qualis_a >= 1 and guidance_cond
        ) or tech_cond:
            return 'B'

    if years_doc >= 6:
        guidance_cond = masters_guidance >= 1 or doc_guidance >= 1
        tech_cond = patent >= 1 or software >= 3
        if (academic_prod >= 3 and qualis_a >= 2 and guidance_cond) or (
            qualis_a >= 1 and tech_cond
        ):
            return 'C+'
        if (
            academic_prod >= 3 and qualis_a >= 1 and guidance_cond
        ) or tech_cond:
            return 'C'

    if years_doc >= 3:
        tech_cond = patent >= 1 or software >= 3
        if (academic_prod >= 2 and qualis_a >= 1) or tech_cond:
            return 'D+'
        if (academic_prod >= 2 and articles >= 1) or tech_cond:
            return 'D'

    if years_doc > 0:
        tech_cond = patent >= 1 or software >= 3
        if academic_prod >= 1 or (qualis_a >= 1 and tech_cond):
            return 'E+'

    return 'E'


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Calcula e atualiza a classificação acadêmica dos pesquisadores."""
    logger.info('Starting classify_researchers routine')
    where_clauses = ['1=1']
    params: dict[str, object] = {}
    if researcher_ids:
        where_clauses.append('id = ANY(:researcher_ids)')
        params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        where_clauses.append('lattes_id = ANY(:lattes_ids)')
        params['lattes_ids'] = list(lattes_ids)

    where_sql = ' AND '.join(where_clauses)
    base_sql = text(
        f'SELECT id::TEXT AS researcher_id '
        f'FROM public.researcher WHERE {where_sql}'
    )
    rows = session.execute(base_sql, params).mappings().all()
    if not rows:
        logger.warning('No researchers found for classification')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.DataFrame(rows).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )

    p_year = {'year': YEAR_FILTER}
    metric_dfs = [
        _article_metrics(session, YEAR_FILTER),
        _patent_metrics(session, YEAR_FILTER),
        _guidance_metrics(session, YEAR_FILTER),
        _education_metrics(session),
        _simple_count_metrics(
            session,
            'SELECT researcher_id::TEXT, COUNT(*) AS software '
            'FROM public.software WHERE year >= :year GROUP BY researcher_id;',
            p_year,
            'software',
        ),
        _simple_count_metrics(
            session,
            'SELECT researcher_id::TEXT, COUNT(*) AS book '
            "FROM bibliographic_production WHERE type = 'BOOK' "
            'AND year_ >= :year GROUP BY researcher_id;',
            p_year,
            'book',
        ),
        _simple_count_metrics(
            session,
            'SELECT researcher_id::TEXT, COUNT(*) AS book_chapter '
            "FROM bibliographic_production WHERE type = 'BOOK_CHAPTER' "
            'AND year_ >= :year GROUP BY researcher_id;',
            p_year,
            'book_chapter',
        ),
    ]

    for m_df in metric_dfs:
        df = df.join(m_df, how='left', on='researcher_id')

    df = df.fill_null(0)
    records = df.to_dicts()

    update_payload = []
    for r in records:
        update_payload.append({
            'researcher_id': r['researcher_id'],
            'class': calculate_rank(r),
        })

    update_sql = text("""
        UPDATE researcher
        SET classification = :class
        WHERE id = :researcher_id;
    """)

    for i in range(0, len(update_payload), BATCH_SIZE):
        batch = update_payload[i : i + BATCH_SIZE]
        session.execute(update_sql, batch)

    session.commit()
    logger.info(f'Classified and updated {len(update_payload)} researchers')
    return Stats(
        found=len(update_payload),
        succeeded=len(update_payload),
        failed=0,
    )
