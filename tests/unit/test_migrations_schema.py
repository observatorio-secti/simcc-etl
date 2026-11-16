import contextlib
import io
import re
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy.dialects import postgresql

from simcc_etl.core.database import ConsistencyReport, EtlRun

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _render_alembic_upgrade_sql() -> str:
    """Renderiza o DDL completo das migrations Alembic em modo offline."""
    output = io.StringIO()
    cfg = Config(str(PROJECT_ROOT / 'alembic.ini'))
    cfg.set_main_option('script_location', str(PROJECT_ROOT / 'migrations'))
    with contextlib.redirect_stdout(output):
        command.upgrade(cfg, 'head', sql=True)
    return output.getvalue()


def _extract_final_column_types(ddl: str, table: str) -> dict[str, str]:
    """Extrai o tipo final de cada coluna de `table` a partir do DDL."""
    types: dict[str, str] = {}

    create_pattern = re.compile(
        rf'CREATE TABLE {table}\s*\((.*?)\);',
        re.DOTALL | re.IGNORECASE,
    )
    for match in create_pattern.finditer(ddl):
        for raw_line in match.group(1).splitlines():
            line = raw_line.strip().rstrip(',')
            col_match = re.match(
                r'^(\w+)\s+(JSONB|JSON)\b', line, re.IGNORECASE
            )
            if col_match:
                types[col_match.group(1)] = col_match.group(2).upper()

    alter_pattern = re.compile(
        rf'ALTER TABLE {table}\s+ALTER COLUMN\s+(\w+)\s+TYPE\s+(JSONB|JSON)\b',
        re.IGNORECASE,
    )
    for match in alter_pattern.finditer(ddl):
        types[match.group(1)] = match.group(2).upper()

    return types


def test_etl_runs_json_columns_match_orm_jsonb():
    """Migrations e ORM EtlRun devem concordar que colunas JSON são JSONB."""
    ddl = _render_alembic_upgrade_sql()
    final_types = _extract_final_column_types(ddl, 'etl_runs')
    dialect = postgresql.dialect()

    for col_name in ('routines', 'lattes_ids', 'steps'):
        orm_type = EtlRun.__table__.c[col_name].type.compile(dialect=dialect)
        assert final_types.get(col_name) == orm_type.upper() == 'JSONB'


def test_consistency_reports_json_columns_match_orm_jsonb():
    """Migrations e ORM ConsistencyReport devem usar JSONB nas colunas JSON."""
    ddl = _render_alembic_upgrade_sql()
    final_types = _extract_final_column_types(ddl, 'consistency_reports')
    dialect = postgresql.dialect()

    for col_name in ('before', 'after', 'result'):
        orm_type = ConsistencyReport.__table__.c[col_name].type.compile(
            dialect=dialect
        )
        assert final_types.get(col_name) == orm_type.upper() == 'JSONB'
