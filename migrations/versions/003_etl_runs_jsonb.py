"""convert etl_runs json columns to jsonb

Revision ID: 003_etl_runs_jsonb
Revises: 002_fix_foment_sequence_code
Create Date: 2026-11-15 23:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '003_etl_runs_jsonb'
down_revision: str | Sequence[str] | None = '002_fix_foment_sequence_code'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JSONB_COLUMNS = ('routines', 'lattes_ids', 'steps')


def upgrade() -> None:
    for col_name in _JSONB_COLUMNS:
        op.alter_column(
            'etl_runs',
            col_name,
            existing_type=sa.JSON(),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            postgresql_using=f'{col_name}::jsonb',
        )


def downgrade() -> None:
    for col_name in _JSONB_COLUMNS:
        op.alter_column(
            'etl_runs',
            col_name,
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=sa.JSON(),
            postgresql_using=f'{col_name}::json',
        )
