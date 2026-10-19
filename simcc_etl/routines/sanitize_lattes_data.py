from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger

logger = get_logger('simcc_etl.routines.sanitize_lattes_data')

SQL_STEPS = [
    (
        'clean_bibliographic_year',
        'UPDATE bibliographic_production SET year = NULL '
        "WHERE year !~ '^\\d+$';",
    ),
    (
        'fix_specific_qualis',
        "UPDATE bibliographic_production_article ba SET qualis = 'A4' "
        "WHERE ba.issn = '17412242';",
    ),
    (
        'set_docente_flag',
        'UPDATE researcher SET docente = true '
        'WHERE id IN (SELECT researcher_id FROM graduate_program_researcher);',
    ),
    (
        'update_jcr_eissn',
        """
        UPDATE bibliographic_production_article p
        SET jcr = subquery.jif2019, jcr_link = subquery.url_revista
        FROM (SELECT jif2019, eissn, url_revista FROM jcr) AS subquery
        WHERE translate(subquery.eissn, '-', '') = p.issn;
        """,
    ),
    (
        'update_jcr_issn',
        """
        UPDATE bibliographic_production_article p
        SET jcr = subquery.jif2019, jcr_link = subquery.url_revista
        FROM (SELECT jif2019, issn, url_revista FROM jcr) AS subquery
        WHERE translate(subquery.issn, '-', '') = p.issn;
        """,
    ),
    (
        'cast_year_integer',
        'UPDATE bibliographic_production SET year_ = year::INTEGER '
        "WHERE year IS NOT NULL AND year ~ '^\\d+$';",
    ),
    (
        'clean_title_quotes',
        'UPDATE bibliographic_production '
        "SET title = translate(title, '''', ' ');",
    ),
    (
        'update_periodical_jcr',
        """
        UPDATE periodical_magazine pm
        SET jcr = jcr.jif2019
        FROM jcr
        WHERE REPLACE(jcr.issn, '-', '') = pm.issn
        AND pm.issn IS NOT NULL;
        """,
    ),
    (
        'split_great_area',
        'UPDATE researcher_production '
        "SET great_area_ = STRING_TO_ARRAY(great_area, ';') "
        'WHERE great_area IS NOT NULL;',
    ),
    (
        'deduplicate_prof_experience',
        """
        WITH ranked AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           researcher_id, enterprise, start_year, end_year
                       ORDER BY id
                   ) AS rn
            FROM public.researcher_professional_experience
        )
        DELETE FROM public.researcher_professional_experience
        WHERE id IN (SELECT id FROM ranked WHERE rn > 1);
        """,
    ),
    (
        'clean_guidance_html_tags',
        'UPDATE guidance '
        "SET title = regexp_replace(title, '<a[^>]*>|</a>', '', 'gi') "
        "WHERE title ~ '<a[^>]*>|</a>';",
    ),
    (
        'link_group_leaders',
        """
        UPDATE research_group rg
        SET second_leader_id = r.id
        FROM researcher r
        WHERE rg.second_leader = r.name;

        UPDATE research_group rg
        SET first_leader_id = r.id
        FROM researcher r
        WHERE rg.first_leader = r.name;
        """,
    ),
    (
        'fix_gp_researchers',
        """
        WITH distinct_researchers AS (
            SELECT DISTINCT ON (graduate_program_id, researcher_id)
                graduate_program_id,
                researcher_id,
                type_,
                tag
            FROM graduate_program_researcher
            ORDER BY graduate_program_id, researcher_id, year DESC
        ),
        years AS (
            SELECT generate_series(2012, 2026) AS year
        )
        INSERT INTO graduate_program_researcher (
            graduate_program_id,
            researcher_id,
            year,
            type_,
            tag
        )
        SELECT
            r.graduate_program_id,
            r.researcher_id,
            y.year,
            r.type_,
            r.tag
        FROM distinct_researchers r
        CROSS JOIN years y
        ON CONFLICT (graduate_program_id, researcher_id, year) DO NOTHING;
        """,
    ),
    (
        'update_quadrennial',
        """
        UPDATE bibliographic_production_article bpa
        SET quadrennial = CASE
            WHEN bp.year_ BETWEEN 2013 AND 2016 THEN '2013/2016'
            WHEN bp.year_ BETWEEN 2017 AND 2020 THEN '2017/2020'
            WHEN bp.year_ BETWEEN 2021 AND 2028 THEN '2021/2024'
            ELSE NULL
        END
        FROM bibliographic_production bp
        WHERE bpa.bibliographic_production_id = bp.id;
        """,
    ),
]


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Executa a sanitização pós-extração Hop nos dados do Lattes."""
    count = len(SQL_STEPS)
    logger.info(f'Starting sanitize_lattes_data routine with {count} steps')
    succeeded = 0
    failed = 0

    for step_name, sql_str in SQL_STEPS:
        try:
            logger.debug(f'Running step {step_name}')
            session.execute(text(sql_str))
            session.commit()
            succeeded += 1
        except Exception as e:
            session.rollback()
            failed += 1
            logger.error(f'Error executing step {step_name}: {e}')
            raise

    logger.info(
        f'Finished sanitize_lattes_data routine: {succeeded}/{count} succeeded'
    )
    return Stats(found=count, succeeded=succeeded, failed=failed)
