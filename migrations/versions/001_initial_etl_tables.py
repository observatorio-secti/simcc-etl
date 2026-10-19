"""initial etl tables

Revision ID: 001_initial_etl_tables
Revises:
Create Date: 2026-10-19 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = '001_initial_etl_tables'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'etl_runs',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='PENDING'),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('ended_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('triggered_by', sa.String(length=255), nullable=False, server_default='system'),
        sa.Column('current_routine', sa.String(length=100), nullable=True),
        sa.Column('routines', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('lattes_ids', sa.JSON(), nullable=True),
        sa.Column('steps', sa.JSON(), nullable=False, server_default='{}'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_etl_runs_status'), 'etl_runs', ['status'], unique=False)
    op.create_table(
        'research_group_ind_prod',
        sa.Column('research_group_id', sa.UUID(), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column(
            'ind_prod_article',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.Column(
            'ind_prod_book', sa.Float(), nullable=False, server_default='0.0'
        ),
        sa.Column(
            'ind_prod_book_chapter',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.Column(
            'ind_prod_software',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.Column(
            'ind_prod_report', sa.Float(), nullable=False, server_default='0.0'
        ),
        sa.Column(
            'ind_prod_granted_patent',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.Column(
            'ind_prod_not_granted_patent',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.Column(
            'ind_prod_guidance',
            sa.Float(),
            nullable=False,
            server_default='0.0',
        ),
        sa.PrimaryKeyConstraint('research_group_id', 'year'),
    )


def downgrade() -> None:
    op.drop_table('research_group_ind_prod')
    op.drop_table('etl_runs')
