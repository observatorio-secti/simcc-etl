import re
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.text import normalize_text

logger = get_logger('simcc_etl.routines.import_program_research_lines')

SEED_CSV_PATH = Path('storage/seed/program_research_lines.csv')


def _clean_whitespace(value: Any) -> str:
    if value is None:
        return ''
    return re.sub(r'\s+', ' ', str(value)).strip()


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Importa linhas de pesquisa dos programas a partir do CSV de seed."""
    logger.info('Starting import_program_research_lines routine')
    if not SEED_CSV_PATH.exists():
        logger.warning(f'Seed file not found: {SEED_CSV_PATH}')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.read_csv(SEED_CSV_PATH)
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

    prog_sql = text(
        'SELECT graduate_program_id, code FROM public.graduate_program;'
    )
    prog_rows = session.execute(prog_sql).mappings().all()
    code_map = {row['code']: row['graduate_program_id'] for row in prog_rows}

    total_rows = len(df)
    deduped: dict[tuple[Any, str], dict[str, Any]] = {}
    failed = 0

    for row in df.to_dicts():
        code = row.get('codigo do programa')
        prog_id = code_map.get(code)
        name = _clean_whitespace(row.get('nome')).title()
        area = _clean_whitespace(row.get('area de concentracao')).upper()

        if not prog_id or not name or not area:
            failed += 1
            continue

        deduped[(prog_id, name)] = {
            'graduate_program_id': prog_id,
            'name': name,
            'area': area,
            'start_year': row.get('start_year'),
            'end_year': row.get('end_year'),
        }

    records_to_upsert = list(deduped.values())
    if records_to_upsert:
        upsert_sql = text("""
            INSERT INTO public.research_lines_programs
                (graduate_program_id, name, area, start_year, end_year)
            VALUES
                (:graduate_program_id, :name, :area, :start_year, :end_year)
            ON CONFLICT (graduate_program_id, name) DO UPDATE SET
                area = EXCLUDED.area,
                start_year = EXCLUDED.start_year,
                end_year = EXCLUDED.end_year;
        """)
        session.execute(upsert_sql, records_to_upsert)
        session.commit()

    succeeded = len(records_to_upsert)
    logger.info(
        f'Finished import_program_research_lines: '
        f'{succeeded}/{total_rows} upserted'
    )
    return Stats(found=total_rows, succeeded=succeeded, failed=failed)
