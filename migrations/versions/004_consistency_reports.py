"""create consistency_reports table

Revision ID: 004_consistency_reports
Revises: 003_etl_runs_jsonb
Create Date: 2026-11-16 10:55:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '004_consistency_reports'
down_revision: str | Sequence[str] | None = '003_etl_runs_jsonb'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'consistency_reports',
        sa.Column('id', sa.String(length=64), nullable=False),
        sa.Column('etl_run_id', sa.UUID(), nullable=True),
        sa.Column('pipeline_log_id', sa.String(length=128), nullable=True),
        sa.Column(
            'scope',
            sa.String(length=20),
            nullable=False,
            server_default='simcc',
        ),
        sa.Column('type', sa.String(length=100), nullable=False),
        sa.Column(
            'origin',
            sa.String(length=32),
            nullable=False,
            server_default='system',
        ),
        sa.Column(
            'coverage',
            sa.String(length=20),
            nullable=False,
            server_default='FULL',
        ),
        sa.Column(
            'status',
            sa.String(length=32),
            nullable=False,
            server_default='IN_PROGRESS',
        ),
        sa.Column(
            'started_at',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('now()'),
        ),
        sa.Column('ended_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('reference_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            'before',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default='{}',
        ),
        sa.Column(
            'after', postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            'result', postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column('error', sa.Text(), nullable=True),
        sa.CheckConstraint(
            "scope = 'simcc'", name='ck_consistency_reports_scope'
        ),
        sa.ForeignKeyConstraint(
            ['etl_run_id'], ['etl_runs.id'], ondelete='SET NULL'
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('pipeline_log_id'),
    )
    op.create_index(
        op.f('ix_consistency_reports_etl_run_id'),
        'consistency_reports',
        ['etl_run_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_consistency_reports_started_at'),
        'consistency_reports',
        ['started_at'],
        unique=False,
    )
    op.create_index(
        op.f('ix_consistency_reports_status'),
        'consistency_reports',
        ['status'],
        unique=False,
    )
    op.create_index(
        op.f('ix_consistency_reports_type'),
        'consistency_reports',
        ['type'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f('ix_consistency_reports_type'), table_name='consistency_reports'
    )
    op.drop_index(
        op.f('ix_consistency_reports_status'),
        table_name='consistency_reports',
    )
    op.drop_index(
        op.f('ix_consistency_reports_started_at'),
        table_name='consistency_reports',
    )
    op.drop_index(
        op.f('ix_consistency_reports_etl_run_id'),
        table_name='consistency_reports',
    )
    op.drop_table('consistency_reports')
