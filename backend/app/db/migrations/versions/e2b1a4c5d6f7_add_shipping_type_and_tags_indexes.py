"""add shipping_type index and product_tags gin index

Revision ID: e2b1a4c5d6f7
Revises: 9d6cf6b17f4e
Create Date: 2025-11-27 00:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'e2b1a4c5d6f7'
down_revision = '9d6cf6b17f4e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        'ix_kogan_sku_freight_fee_shipping_type',
        'kogan_sku_freight_fee',
        ['shipping_type'],
        unique=False,
    )

    op.create_index(
        'gin_sku_info_product_tags',
        'sku_info',
        ['product_tags'],
        unique=False,
        postgresql_using='gin',
    )


def downgrade() -> None:
    op.drop_index('gin_sku_info_product_tags', table_name='sku_info')
    op.drop_index('ix_kogan_sku_freight_fee_shipping_type', table_name='kogan_sku_freight_fee')
