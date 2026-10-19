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
        sa.Column('routine_name', sa.String(length=100), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('items_found', sa.Integer(), nullable=False),
        sa.Column('items_succeeded', sa.Integer(), nullable=False),
        sa.Column('items_failed', sa.Integer(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
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
