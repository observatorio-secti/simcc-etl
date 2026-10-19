from datetime import datetime
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.text import strip_accents

logger = get_logger('simcc_etl.routines.calculate_production_index')

BAREMA: dict[str, float] = {
    'A1': 1.0,
    'A2': 0.875,
    'A3': 0.75,
    'A4': 0.625,
    'B1': 0.5,
    'B2': 0.375,
    'B3': 0.25,
    'B4': 0.125,
    'C': 0.0,
    'SQ': 0.0,
    'BOOK': 1.0,
    'BOOK_CHAPTER': 0.25,
    'SOFTWARE': 0.25,
    'PATENT_GRANTED': 1.0,
    'PATENT_NOT_GRANTED': 0.25,
    'REPORT': 0.25,
    'TESE DE DOUTORADO CONCLUIDA': 0.5,
    'TESE DE DOUTORADO EM ANDAMENTO': 0.25,
    'DISSERTACAO DE MESTRADO CONCLUIDA': 0.25,
    'DISSERTACAO DE MESTRADO EM ANDAMENTO': 0.125,
    'INICIACAO CIENTIFICA CONCLUIDA': 0.125,
    'INICIACAO CIENTIFICA EM ANDAMENTO': 0.1,
}

START_YEAR = 2008
BATCH_SIZE = 5000


def _fetch_articles(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT
            COALESCE(bp.year_, bp.year::INTEGER) AS year,
            bpa.qualis,
            COUNT(*) AS count_article,
            bp.researcher_id::TEXT
        FROM bibliographic_production bp
        JOIN bibliographic_production_article bpa
          ON bp.id = bpa.bibliographic_production_id
        WHERE (bp.year_ IS NOT NULL
               OR (bp.year IS NOT NULL AND bp.year ~ '^\\d+$'))
        GROUP BY COALESCE(bp.year_, bp.year::INTEGER),
                 bpa.qualis,
                 bp.researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'article_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows)
    df = df.with_columns(
        (
            pl.col('qualis')
            .replace_strict(BAREMA, default=0.0)
            .cast(pl.Float64)
            * pl.col('count_article')
        ).alias('article_prod'),
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )
    return (
        df.group_by(['year', 'researcher_id'])
        .agg(pl.col('article_prod').sum())
        .select(['year', 'researcher_id', 'article_prod'])
    )


def _fetch_books(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT
            COALESCE(bp.year_, bp.year::INTEGER) AS year,
            COUNT(*) AS count_book,
            bp.researcher_id::TEXT
        FROM bibliographic_production bp
        WHERE bp.type = 'BOOK'
          AND (bp.year_ IS NOT NULL
               OR (bp.year IS NOT NULL AND bp.year ~ '^\\d+$'))
        GROUP BY COALESCE(bp.year_, bp.year::INTEGER), bp.researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'book_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows).with_columns(
        (pl.col('count_book') * BAREMA.get('BOOK', 1.0)).alias('book_prod'),
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )
    return df.select(['year', 'researcher_id', 'book_prod'])


def _fetch_book_chapters(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT
            COALESCE(bp.year_, bp.year::INTEGER) AS year,
            COUNT(*) AS count_book_chapter,
            bp.researcher_id::TEXT
        FROM bibliographic_production bp
        WHERE bp.type = 'BOOK_CHAPTER'
          AND (bp.year_ IS NOT NULL
               OR (bp.year IS NOT NULL AND bp.year ~ '^\\d+$'))
        GROUP BY COALESCE(bp.year_, bp.year::INTEGER), bp.researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'book_chapter_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    alias_name = 'book_chapter_prod'
    df = pl.DataFrame(rows).with_columns(
        (
            pl.col('count_book_chapter') * BAREMA.get('BOOK_CHAPTER', 0.25)
        ).alias(alias_name),
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )
    return df.select(['year', 'researcher_id', 'book_chapter_prod'])


def fetch_patents(
    session: Session,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    sql = text("""
        SELECT
            development_year AS year,
            CASE
                WHEN grant_date IS NOT NULL THEN 'PATENT_GRANTED'
                ELSE 'PATENT_NOT_GRANTED'
            END AS patent_type,
            researcher_id::TEXT,
            COUNT(*) AS count_patent
        FROM patent
        WHERE development_year IS NOT NULL
        GROUP BY development_year, patent_type, researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    granted_schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'patent_prod_granted': pl.Float64,
    }
    not_granted_schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'patent_prod_not_granted': pl.Float64,
    }
    if not rows:
        return (
            pl.DataFrame(schema=granted_schema),
            pl.DataFrame(schema=not_granted_schema),
        )

    df = pl.DataFrame(rows).with_columns(
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )

    granted_df = (
        df.filter(pl.col('patent_type') == 'PATENT_GRANTED')
        .with_columns(
            (pl.col('count_patent') * BAREMA.get('PATENT_GRANTED', 1.0)).alias(
                'patent_prod_granted'
            )
        )
        .select(['year', 'researcher_id', 'patent_prod_granted'])
    )

    not_granted_df = (
        df.filter(pl.col('patent_type') == 'PATENT_NOT_GRANTED')
        .with_columns(
            (
                pl.col('count_patent') * BAREMA.get('PATENT_NOT_GRANTED', 0.25)
            ).alias('patent_prod_not_granted')
        )
        .select(['year', 'researcher_id', 'patent_prod_not_granted'])
    )

    return granted_df, not_granted_df


def _fetch_software(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT year, COUNT(*) AS count_software, researcher_id::TEXT
        FROM software
        WHERE year IS NOT NULL
        GROUP BY year, researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'software_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows).with_columns(
        (pl.col('count_software') * BAREMA.get('SOFTWARE', 0.25)).alias(
            'software_prod'
        ),
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )
    return df.select(['year', 'researcher_id', 'software_prod'])


def _fetch_reports(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT year, COUNT(*) AS count_report, researcher_id::TEXT
        FROM research_report
        WHERE year IS NOT NULL
        GROUP BY year, researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'report_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(rows).with_columns(
        (pl.col('count_report') * BAREMA.get('REPORT', 0.25)).alias(
            'report_prod'
        ),
        pl.col('year').cast(pl.Int64),
        pl.col('researcher_id').cast(pl.String),
    )
    return df.select(['year', 'researcher_id', 'report_prod'])


def _fetch_guidance(session: Session) -> pl.DataFrame:
    sql = text("""
        SELECT year, nature, status, COUNT(*) AS count_guidance,
               researcher_id::TEXT
        FROM guidance
        WHERE year IS NOT NULL
        GROUP BY year, nature, status, researcher_id;
    """)
    rows = session.execute(sql).mappings().all()
    schema = {
        'year': pl.Int64,
        'researcher_id': pl.String,
        'guidance_prod': pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    def _calc_guidance_weight(row: dict[str, Any]) -> float:
        nat = row.get('nature') or ''
        st = row.get('status') or ''
        key = strip_accents(f'{nat} {st}').upper().strip()
        weight = BAREMA.get(key, 0.0)
        return float(weight * (row.get('count_guidance') or 0))

    data = []
    for r in rows:
        data.append({
            'year': int(r['year']),
            'researcher_id': str(r['researcher_id']),
            'guidance_prod': _calc_guidance_weight(r),
        })

    df = pl.DataFrame(data)
    return (
        df.group_by(['year', 'researcher_id'])
        .agg(pl.col('guidance_prod').sum())
        .select(['year', 'researcher_id', 'guidance_prod'])
    )


def _build_researcher_metrics_df(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> pl.DataFrame:
    """Consolida as métricas individuais de produção por pesquisador e ano."""
    where_clauses = ['1=1']
    params: dict[str, object] = {}
    if researcher_ids:
        where_clauses.append('id = ANY(:researcher_ids)')
        params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        where_clauses.append('lattes_id = ANY(:lattes_ids)')
        params['lattes_ids'] = list(lattes_ids)

    where_sql = ' AND '.join(where_clauses)
    res_sql = text(
        'SELECT id::TEXT AS researcher_id '
        f'FROM public.researcher WHERE {where_sql}'
    )
    researcher_rows = session.execute(res_sql, params).mappings().all()
    if not researcher_rows:
        return pl.DataFrame()

    current_year = datetime.now().year
    years = list(range(START_YEAR, current_year + 1))
    history_df = pl.DataFrame({'year': years}).with_columns(
        pl.col('year').cast(pl.Int64)
    )

    grid = (
        pl.DataFrame(researcher_rows)
        .with_columns(pl.col('researcher_id').cast(pl.String))
        .join(history_df, how='cross')
    )

    granted_pat, not_granted_pat = fetch_patents(session)
    on = ['researcher_id', 'year']
    return (
        grid.join(_fetch_articles(session), on=on, how='left')
        .join(_fetch_books(session), on=on, how='left')
        .join(_fetch_book_chapters(session), on=on, how='left')
        .join(granted_pat, on=on, how='left')
        .join(not_granted_pat, on=on, how='left')
        .join(_fetch_software(session), on=on, how='left')
        .join(_fetch_reports(session), on=on, how='left')
        .join(_fetch_guidance(session), on=on, how='left')
        .fill_null(0.0)
    )


def run_researcher_indprod(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Calcula e persiste o índice de produção para pesquisadores."""
    logger.info('Calculating researcher IndProd')
    df = _build_researcher_metrics_df(session, researcher_ids, lattes_ids)
    if df.is_empty():
        logger.warning('No researchers found for IndProd calculation')
        return Stats(found=0, succeeded=0, failed=0)

    where_clauses = ['1=1']
    params: dict[str, object] = {}
    if researcher_ids:
        where_clauses.append('researcher_id = ANY(:researcher_ids)')
        params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        subquery = (
            'SELECT id FROM researcher WHERE lattes_id = ANY(:lattes_ids)'
        )
        where_clauses.append(f'researcher_id IN ({subquery})')
        params['lattes_ids'] = list(lattes_ids)

    where_sql = ' AND '.join(where_clauses)
    del_sql = text(f'DELETE FROM researcher_ind_prod WHERE {where_sql};')
    session.execute(del_sql, params)

    insert_sql = text("""
        INSERT INTO researcher_ind_prod (
            researcher_id, year,
            ind_prod_article, ind_prod_book, ind_prod_book_chapter,
            ind_prod_software, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance
        ) VALUES (
            :researcher_id, :year,
            :article_prod, :book_prod, :book_chapter_prod,
            :software_prod, :patent_prod_granted,
            :patent_prod_not_granted, :report_prod, :guidance_prod
        );
    """)

    records = df.to_dicts()
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        session.execute(insert_sql, batch)

    session.commit()
    logger.info(f'Inserted {len(records)} researcher_ind_prod records')
    return Stats(found=len(records), succeeded=len(records), failed=0)


def run_graduate_program_indprod(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Calcula e persiste o índice de produção para pós-graduações."""
    logger.info('Calculating graduate program IndProd')
    programs_sql = text("""
        SELECT DISTINCT graduate_program_id::TEXT, researcher_id::TEXT
        FROM graduate_program_researcher;
    """)
    program_rows = session.execute(programs_sql).mappings().all()
    if not program_rows:
        logger.warning('No graduate programs found')
        return Stats(found=0, succeeded=0, failed=0)

    programs_df = pl.DataFrame(program_rows).with_columns(
        pl.col('graduate_program_id').cast(pl.String),
        pl.col('researcher_id').cast(pl.String),
    )

    researchers_metrics = _build_researcher_metrics_df(session)
    if researchers_metrics.is_empty():
        return Stats(found=0, succeeded=0, failed=0)

    aggregated = (
        programs_df.join(researchers_metrics, on='researcher_id', how='inner')
        .drop('researcher_id')
        .group_by(['graduate_program_id', 'year'])
        .sum()
        .sort(['graduate_program_id', 'year'])
        .fill_null(0.0)
    )

    session.execute(text('DELETE FROM graduate_program_ind_prod;'))

    insert_sql = text("""
        INSERT INTO graduate_program_ind_prod (
            graduate_program_id, year,
            ind_prod_article, ind_prod_book, ind_prod_book_chapter,
            ind_prod_software, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance
        ) VALUES (
            :graduate_program_id, :year,
            :article_prod, :book_prod, :book_chapter_prod,
            :software_prod, :patent_prod_granted,
            :patent_prod_not_granted, :report_prod, :guidance_prod
        );
    """)

    records = aggregated.to_dicts()
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        session.execute(insert_sql, batch)

    session.commit()
    logger.info(f'Inserted {len(records)} graduate_program_ind_prod records')
    return Stats(found=len(records), succeeded=len(records), failed=0)


def run_research_group_indprod(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Calcula e persiste o índice de produção para grupos de pesquisa."""
    logger.info('Calculating research group IndProd')
    groups_sql = text("""
        SELECT DISTINCT research_group_id::TEXT, researcher_id::TEXT
        FROM (
            SELECT research_group_id, researcher_id
            FROM research_group_researcher
            UNION
            SELECT id AS research_group_id, first_leader_id AS researcher_id
            FROM research_group WHERE first_leader_id IS NOT NULL
            UNION
            SELECT id AS research_group_id, second_leader_id AS researcher_id
            FROM research_group WHERE second_leader_id IS NOT NULL
        ) grp;
    """)
    group_rows = session.execute(groups_sql).mappings().all()
    if not group_rows:
        logger.warning('No research groups or members found')
        return Stats(found=0, succeeded=0, failed=0)

    groups_df = pl.DataFrame(group_rows).with_columns(
        pl.col('research_group_id').cast(pl.String),
        pl.col('researcher_id').cast(pl.String),
    )

    researchers_metrics = _build_researcher_metrics_df(session)
    if researchers_metrics.is_empty():
        return Stats(found=0, succeeded=0, failed=0)

    aggregated = (
        groups_df.join(researchers_metrics, on='researcher_id', how='inner')
        .drop('researcher_id')
        .group_by(['research_group_id', 'year'])
        .sum()
        .sort(['research_group_id', 'year'])
        .fill_null(0.0)
    )

    insert_sql = text("""
        INSERT INTO research_group_ind_prod (
            research_group_id, year,
            ind_prod_article, ind_prod_book, ind_prod_book_chapter,
            ind_prod_software, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance
        ) VALUES (
            :research_group_id, :year,
            :article_prod, :book_prod, :book_chapter_prod,
            :software_prod, :patent_prod_granted,
            :patent_prod_not_granted, :report_prod, :guidance_prod
        )
        ON CONFLICT (research_group_id, year) DO UPDATE SET
            ind_prod_article = EXCLUDED.ind_prod_article,
            ind_prod_book = EXCLUDED.ind_prod_book,
            ind_prod_book_chapter = EXCLUDED.ind_prod_book_chapter,
            ind_prod_software = EXCLUDED.ind_prod_software,
            ind_prod_granted_patent = EXCLUDED.ind_prod_granted_patent,
            ind_prod_not_granted_patent = EXCLUDED.ind_prod_not_granted_patent,
            ind_prod_report = EXCLUDED.ind_prod_report,
            ind_prod_guidance = EXCLUDED.ind_prod_guidance;
    """)

    records = aggregated.to_dicts()
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        session.execute(insert_sql, batch)

    session.commit()
    logger.info(f'Inserted {len(records)} research_group_ind_prod records')
    return Stats(found=len(records), succeeded=len(records), failed=0)


def run_all(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Executa a cadeia completa de cálculo de índice de produção."""
    s1 = run_researcher_indprod(session, researcher_ids, lattes_ids)
    s2 = run_graduate_program_indprod(session, researcher_ids, lattes_ids)
    s3 = run_research_group_indprod(session, researcher_ids, lattes_ids)
    return Stats(
        found=s1.found + s2.found + s3.found,
        succeeded=s1.succeeded + s2.succeeded + s3.succeeded,
        failed=s1.failed + s2.failed + s3.failed,
    )
