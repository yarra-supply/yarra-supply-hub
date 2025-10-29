"""expand attrs_hash_last_calc length

Revision ID: 4fded3a2f8c2
Revises: 7ccaf0a51b0f
Create Date: 2025-10-29 12:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4fded3a2f8c2'
down_revision = '7ccaf0a51b0f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        'kogan_sku_freight_fee',
        'attrs_hash_last_calc',
        type_=sa.String(length=128),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        'kogan_sku_freight_fee',
        'attrs_hash_last_calc',
        type_=sa.String(length=32),
        existing_nullable=True,
    )
