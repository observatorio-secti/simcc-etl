"""fix foment sequence code default and nullability

Revision ID: 002_fix_foment_sequence_code
Revises: 001_initial_etl_tables
Create Date: 2026-10-19 02:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '002_fix_foment_sequence_code'
down_revision: Union[str, Sequence[str], None] = '001_initial_etl_tables'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'foment',
        'sequence_code',
        existing_type=sa.Integer(),
        nullable=True,
        server_default=sa.text('0'),
    )


def downgrade() -> None:
    op.alter_column(
        'foment',
        'sequence_code',
        existing_type=sa.Integer(),
        nullable=False,
        server_default=None,
    )
