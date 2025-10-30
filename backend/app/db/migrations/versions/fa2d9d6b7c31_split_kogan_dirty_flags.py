"""split kogan dirty flags per country

Revision ID: fa2d9d6b7c31
Revises: f3c1c8c0a123
Create Date: 2025-10-30 13:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


revision = 'fa2d9d6b7c31'
down_revision = 'f3c1c8c0a123'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'kogan_sku_freight_fee',
        sa.Column('kogan_dirty_au', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )
    op.add_column(
        'kogan_sku_freight_fee',
        sa.Column('kogan_dirty_nz', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )

    op.execute(
        "UPDATE kogan_sku_freight_fee SET kogan_dirty_au = kogan_dirty, kogan_dirty_nz = kogan_dirty"
    )

    op.create_index(
        'ix_kogan_dirty_au_true_only',
        'kogan_sku_freight_fee',
        ['sku_code'],
        unique=False,
        postgresql_where=sa.text('kogan_dirty_au = true')
    )
    op.create_index(
        'ix_kogan_dirty_nz_true_only',
        'kogan_sku_freight_fee',
        ['sku_code'],
        unique=False,
        postgresql_where=sa.text('kogan_dirty_nz = true')
    )

    op.drop_index('ix_kogan_dirty_true_only', table_name='kogan_sku_freight_fee')
    op.drop_index('ix_kogan_dirty_source', table_name='kogan_sku_freight_fee')
    op.drop_column('kogan_sku_freight_fee', 'kogan_dirty')


def downgrade() -> None:
    op.add_column(
        'kogan_sku_freight_fee',
        sa.Column('kogan_dirty', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )
    op.create_index(
        'ix_kogan_dirty_true_only',
        'kogan_sku_freight_fee',
        ['sku_code'],
        unique=False,
        postgresql_where=sa.text('kogan_dirty = true')
    )

    op.create_index(
        'ix_kogan_dirty_source',
        'kogan_sku_freight_fee',
        ['kogan_dirty', 'last_changed_source'],
        unique=False,
    )

    op.drop_index('ix_kogan_dirty_au_true_only', table_name='kogan_sku_freight_fee')
    op.drop_index('ix_kogan_dirty_nz_true_only', table_name='kogan_sku_freight_fee')

    op.drop_column('kogan_sku_freight_fee', 'kogan_dirty_nz')
    op.drop_column('kogan_sku_freight_fee', 'kogan_dirty_au')
