import json

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.database import get_admin_engine
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


def sync_admin_data(
    write_session: Session,
    admin_engine: Engine | None = None,
) -> None:
    """Sincroniza dados do admin_db para o write_db."""
    if admin_engine is None:
        admin_engine = get_admin_engine()

    logger.info(
        'Iniciando sincronização de metadados do admin_db para write_db'
    )

    with admin_engine.connect() as admin_conn:
        admin_researchers = (
            admin_conn.execute(
                text(
                    'SELECT id, name, lattes_id '
                    'FROM academic.researchers '
                    'WHERE lattes_id IS NOT NULL'
                )
            )
            .mappings()
            .all()
        )

        admin_institutions = (
            admin_conn.execute(
                text(
                    'SELECT name, acronym FROM academic.institutions '
                    'WHERE name IS NOT NULL OR acronym IS NOT NULL'
                )
            )
            .mappings()
            .all()
        )

        admin_collections = (
            admin_conn.execute(
                text(
                    'SELECT id, slug, name, description, '
                    'refresh_interval_days, is_default, created_at '
                    'FROM academic.collections'
                )
            )
            .mappings()
            .all()
        )

        admin_ri_links = (
            admin_conn.execute(
                text(
                    'SELECT DISTINCT r.lattes_id, i.acronym '
                    'FROM academic.researcher_institutions ri '
                    'JOIN academic.researchers r ON ri.researcher_id = r.id '
                    'JOIN academic.institutions i ON ri.institution_id = i.id '
                    'WHERE r.lattes_id IS NOT NULL AND i.acronym IS NOT NULL'
                )
            )
            .mappings()
            .all()
        )

        admin_rc_links = (
            admin_conn.execute(
                text(
                    'SELECT r.lattes_id, rc.collection_id, '
                    'rc.attributes, rc.created_at '
                    'FROM academic.researcher_collections rc '
                    'JOIN academic.researchers r ON rc.researcher_id = r.id '
                    'WHERE r.lattes_id IS NOT NULL'
                )
            )
            .mappings()
            .all()
        )

    # a0) Garantir pesquisadores do admin_db em public.researcher
    logger.info(
        f'Sincronizando {len(admin_researchers)} pesquisadores do admin_db para write_db'
    )
    batch_size = 5000
    for i in range(0, len(admin_researchers), batch_size):
        chunk = admin_researchers[i : i + batch_size]
        write_session.execute(
            text(
                """
                INSERT INTO public.researcher (
                    id, name, lattes_id, last_update, has_image, status, stars
                )
                VALUES (:id, :name, :lattes_id, now(), false, true, 0)
                ON CONFLICT (lattes_id) DO NOTHING;
                """
            ),
            chunk,
        )
    write_session.commit()

    # a) Garantir instituições em public.institution (por acronym ou name)
    existing_insts = (
        write_session.execute(
            text('SELECT id, name, acronym FROM public.institution')
        )
        .mappings()
        .all()
    )
    by_acronym = {
        row['acronym']: row['id'] for row in existing_insts if row['acronym']
    }
    by_name = {
        row['name'].lower(): row['id'] for row in existing_insts if row['name']
    }

    for inst in admin_institutions:
        name = inst['name']
        acronym = inst['acronym']
        inst_id = (by_acronym.get(acronym) if acronym else None) or (
            by_name.get(name.lower()) if name else None
        )
        if not inst_id:
            write_session.execute(
                text(
                    'INSERT INTO public.institution (name, acronym) '
                    'VALUES (:name, :acronym)'
                ),
                {'name': name, 'acronym': acronym},
            )
        elif acronym and not by_acronym.get(acronym):
            write_session.execute(
                text(
                    'UPDATE public.institution SET acronym = :acronym '
                    'WHERE id = :id AND acronym IS NULL'
                ),
                {'acronym': acronym, 'id': inst_id},
            )
    write_session.commit()

    # b) Coleções: lê academic.collections e garante em public.collection
    write_session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.collection (
                id UUID PRIMARY KEY,
                slug VARCHAR NOT NULL UNIQUE,
                name VARCHAR NOT NULL,
                description TEXT,
                refresh_interval_days INTEGER NOT NULL DEFAULT 1
                    CHECK (refresh_interval_days >= 1),
                is_default BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
            );
            """
        )
    )
    # Garante que os IDs e slugs de coleções reflitam exatamente o admin_db
    write_session.execute(text('TRUNCATE TABLE public.researcher_collection CASCADE;'))
    write_session.execute(text('DELETE FROM public.collection;'))
    for col in admin_collections:
        write_session.execute(
            text(
                """
                INSERT INTO public.collection (
                    id, slug, name, description,
                    refresh_interval_days, is_default, created_at
                ) VALUES (
                    :id, :slug, :name, :description,
                    :refresh_interval_days, :is_default, :created_at
                )
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    refresh_interval_days = EXCLUDED.refresh_interval_days,
                    is_default = EXCLUDED.is_default;
                """
            ),
            {
                'id': col['id'],
                'slug': col['slug'],
                'name': col['name'],
                'description': col['description'],
                'refresh_interval_days': col['refresh_interval_days'],
                'is_default': col['is_default'],
                'created_at': col['created_at'],
            },
        )
    write_session.commit()

    # c) Vínculos de Pesquisadores e Instituições
    ri_params = [
        {'lattes_id': link['lattes_id'], 'acronym': link['acronym']}
        for link in admin_ri_links
    ]
    for i in range(0, len(ri_params), batch_size):
        write_session.execute(
            text(
                """
                INSERT INTO public.researcher_institution (
                    researcher_id, institution_id
                )
                SELECT r.id, i.id
                FROM public.researcher r
                CROSS JOIN public.institution i
                WHERE r.lattes_id = :lattes_id AND i.acronym = :acronym
                ON CONFLICT (researcher_id, institution_id) DO NOTHING;
                """
            ),
            ri_params[i : i + batch_size],
        )
    write_session.commit()

    # d) Vínculos de Pesquisadores e Coleções
    rc_params = [
        {
            'lattes_id': rc['lattes_id'],
            'collection_id': rc['collection_id'],
            'attributes': (
                json.dumps(rc['attributes'])
                if isinstance(rc['attributes'], dict)
                else (rc['attributes'] or '{}')
            ),
            'created_at': rc['created_at'],
        }
        for rc in admin_rc_links
    ]
    for i in range(0, len(rc_params), batch_size):
        write_session.execute(
            text(
                """
                INSERT INTO public.researcher_collection (
                    researcher_id, collection_id, attributes, created_at
                )
                SELECT
                    r.id, :collection_id,
                    CAST(:attributes AS JSONB), :created_at
                FROM public.researcher r
                WHERE r.lattes_id = :lattes_id
                ON CONFLICT (researcher_id, collection_id) DO UPDATE
                SET attributes = EXCLUDED.attributes;
                """
            ),
            rc_params[i : i + batch_size],
        )
    write_session.commit()
    logger.info('Sincronização de metadados do admin concluída com sucesso')


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    admin_engine: Engine | None = None,
) -> Stats:
    """Executa a sanitização pós-Hop e sincronização com admin_db."""
    sync_admin_data(session, admin_engine=admin_engine)

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
