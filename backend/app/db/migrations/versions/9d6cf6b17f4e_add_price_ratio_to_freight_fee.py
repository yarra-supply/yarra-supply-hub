"""add price_ratio column to freight fee results

Revision ID: 9d6cf6b17f4e
Revises: 4fded3a2f8c2
Create Date: 2025-10-29 12:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9d6cf6b17f4e'
down_revision = '4fded3a2f8c2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'kogan_sku_freight_fee',
        sa.Column('price_ratio', sa.Numeric(10, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('kogan_sku_freight_fee', 'price_ratio')
