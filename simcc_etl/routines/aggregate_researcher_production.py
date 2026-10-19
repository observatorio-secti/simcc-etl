from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger

logger = get_logger('simcc_etl.routines.aggregate_researcher_production')


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Calcula e persiste o resumo quantitativo de produções."""
    logger.info('Starting aggregate_researcher_production routine')

    where_clauses = ['1=1']
    params: dict[str, object] = {}

    if researcher_ids:
        where_clauses.append('r.id = ANY(:researcher_ids)')
        params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        where_clauses.append('r.lattes_id = ANY(:lattes_ids)')
        params['lattes_ids'] = list(lattes_ids)

    where_sql = ' AND '.join(where_clauses)

    delete_sql = f"""
        DELETE FROM researcher_production rp
        USING researcher r
        WHERE rp.researcher_id = r.id AND {where_sql};
    """

    insert_sql = f"""
        WITH bp_counts AS (
            SELECT
                researcher_id,
                COALESCE(
                    COUNT(*) FILTER (WHERE type = 'ARTICLE'), 0
                ) AS articles,
                COALESCE(
                    COUNT(*) FILTER (WHERE type = 'BOOK_CHAPTER'), 0
                ) AS book_chapters,
                COALESCE(
                    COUNT(*) FILTER (WHERE type = 'BOOK'), 0
                ) AS book,
                COALESCE(
                    COUNT(*) FILTER (WHERE type = 'WORK_IN_EVENT'), 0
                ) AS work_in_event
            FROM bibliographic_production
            GROUP BY researcher_id
        ),
        software_counts AS (
            SELECT researcher_id, COUNT(*) AS software
            FROM software
            GROUP BY researcher_id
        ),
        brand_counts AS (
            SELECT researcher_id, COUNT(*) AS brand
            FROM brand
            GROUP BY researcher_id
        ),
        patent_counts AS (
            SELECT researcher_id, COUNT(*) AS patent
            FROM patent
            GROUP BY researcher_id
        ),
        great_areas AS (
            SELECT
                r_in.researcher_id,
                STRING_AGG(DISTINCT gae.name, ';') AS area
            FROM great_area_expertise gae
            JOIN researcher_area_expertise r_in
              ON gae.id = r_in.great_area_expertise_id
            GROUP BY r_in.researcher_id
        ),
        specialties AS (
            SELECT
                r_in.researcher_id,
                STRING_AGG(
                    asp.name || ' | ' || ae.name, '; '
                ) AS area_specialty
            FROM researcher_area_expertise r_in
            JOIN area_specialty asp ON asp.id = r_in.area_specialty_id
            LEFT JOIN area_expertise ae ON r_in.area_expertise_id = ae.id
            GROUP BY r_in.researcher_id
        ),
        addresses AS (
            SELECT DISTINCT ON (researcher_id) researcher_id, city, organ
            FROM researcher_address
            ORDER BY researcher_id
        )
        INSERT INTO researcher_production (
            researcher_id, articles, book_chapters, book, work_in_event,
            patent, software, brand, great_area, great_area_,
            area_specialty, city, organ
        )
        SELECT
            r.id AS researcher_id,
            COALESCE(bp.articles, 0),
            COALESCE(bp.book_chapters, 0),
            COALESCE(bp.book, 0),
            COALESCE(bp.work_in_event, 0),
            COALESCE(pat.patent, 0),
            COALESCE(sw.software, 0),
            COALESCE(br.brand, 0),
            ga.area AS great_area,
            CASE
                WHEN ga.area IS NOT NULL
                THEN STRING_TO_ARRAY(ga.area, ';')
                ELSE NULL
            END AS great_area_,
            sp.area_specialty,
            addr.city,
            addr.organ
        FROM researcher r
        LEFT JOIN bp_counts bp ON bp.researcher_id = r.id
        LEFT JOIN software_counts sw ON sw.researcher_id = r.id
        LEFT JOIN brand_counts br ON br.researcher_id = r.id
        LEFT JOIN patent_counts pat ON pat.researcher_id = r.id
        LEFT JOIN great_areas ga ON ga.researcher_id = r.id
        LEFT JOIN specialties sp ON sp.researcher_id = r.id
        LEFT JOIN addresses addr ON addr.researcher_id = r.id
        WHERE {where_sql}
        RETURNING researcher_id;
    """

    try:
        session.execute(text(delete_sql), params)
        res = session.execute(text(insert_sql), params)
        rows_inserted = len(res.fetchall())
        session.commit()
        logger.info(
            f'Finished aggregate_researcher_production: '
            f'{rows_inserted} researchers inserted'
        )
        return Stats(found=rows_inserted, succeeded=rows_inserted, failed=0)
    except Exception as e:
        session.rollback()
        logger.error(f'Error aggregating researcher production: {e}')
        raise
