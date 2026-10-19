from pathlib import Path

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.text import normalize_text

logger = get_logger('simcc_etl.routines.import_program_research_lines')


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Importa linhas de pesquisa dos programas a partir do CSV de seed."""
    logger.info('Starting import_program_research_lines routine')
    csv_path = Path('storage/seed/program_research_lines.csv')
    if not csv_path.exists():
        logger.warning(f'Seed file not found: {csv_path}')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.read_csv(csv_path)
    df = df.rename({
        col: normalize_text(col).replace('\n', '') for col in df.columns
    })

    df = df.with_columns(
        pl.col('data de inicio')
        .str.to_date(format='%d/%m/%Y', strict=False)
        .dt.year()
        .alias('start_year'),
        pl.col('data de fim')
        .str.to_date(format='%d/%m/%Y', strict=False)
        .dt.year()
        .alias('end_year'),
    )

    prog_sql = text('SELECT graduate_program_id, code FROM graduate_program;')
    prog_rows = session.execute(prog_sql).mappings().all()
    code_map = {row['code']: row['graduate_program_id'] for row in prog_rows}

    total_rows = len(df)
    records_to_insert = []
    failed = 0

    for row in df.to_dicts():
        code = row.get('codigo do programa')
        prog_id = code_map.get(code)
        if not prog_id:
            failed += 1
            continue

        records_to_insert.append({
            'graduate_program_id': prog_id,
            'name': row.get('nome'),
            'area': row.get('area de concentracao'),
            'start_year': row.get('start_year'),
            'end_year': row.get('end_year'),
        })

    if records_to_insert:
        insert_sql = text("""
            INSERT INTO public.research_lines_programs
                (graduate_program_id, name, area, start_year, end_year)
            VALUES
                (:graduate_program_id, :name, :area, :start_year, :end_year)
            ON CONFLICT DO NOTHING;
        """)
        session.execute(insert_sql, records_to_insert)

        format_sql = text("""
            UPDATE research_lines_programs SET
                name = INITCAP(name),
                area = UPPER(area);
        """)
        session.execute(format_sql)
        session.commit()

    succeeded = len(records_to_insert)
    logger.info(
        f'Finished import_program_research_lines: '
        f'{succeeded}/{total_rows} inserted'
    )
    return Stats(found=total_rows, succeeded=succeeded, failed=failed)
