"""add kogan_template_nz

Revision ID: 57517cf4becd
Revises: fa2d9d6b7c31
Create Date: 2025-11-01 01:32:20.556652

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "57517cf4becd"
down_revision = "fa2d9d6b7c31"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. 原 kogan_template 表重命名为 kogan_template_au
    op.rename_table("kogan_template", "kogan_template_au")

    # 2. 更新索引名称（原索引仍存在但表名改变）
    op.drop_index("ix_kogan_template_sku", table_name="kogan_template_au")
    op.drop_index("ix_kogan_template_sku_unique", table_name="kogan_template_au")
    op.create_index(
        "ix_kogan_template_au_sku",
        "kogan_template_au",
        ["sku"],
        unique=False,
    )

    # 3. 创建 kogan_template_NZ（先以字符串列占位，稍后转换回 enum）
    op.create_table(
        "kogan_template_nz",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("sku", sa.String(length=128), nullable=False),
        sa.Column("price", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("rrp", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("kogan_first_price", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("shipping", sa.String(length=128), nullable=True),
        sa.Column("handling_days", sa.Integer(), nullable=True),
        sa.Column("country_type", sa.String(length=2), server_default="NZ", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_kogan_template_NZ")),
    )
    op.create_index("ix_kogan_template_nz_sku", "kogan_template_nz", ["sku"], unique=False)

    # 3.1 将 country_type 列转换为已有的枚举类型，并恢复默认值
    country_enum = postgresql.ENUM("AU", "NZ", name="country_type_enum", create_type=False)
    op.alter_column(
        "kogan_template_nz",
        "country_type",
        server_default=None,
    )
    op.alter_column(
        "kogan_template_nz",
        "country_type",
        type_=country_enum,
        postgresql_using="country_type::country_type_enum",
    )
    op.alter_column(
        "kogan_template_nz",
        "country_type",
        server_default=sa.text("'NZ'::country_type_enum"),
    )

    # 4. 历史数据由业务流程重新生成，此处不迁移


def downgrade() -> None:
    # 1. 删除 NZ 表
    op.drop_index("ix_kogan_template_nz_sku", table_name="kogan_template_nz")
    op.drop_table("kogan_template_nz")

    # 2. 恢复 AU 表索引
    op.drop_index("ix_kogan_template_au_sku", table_name="kogan_template_au")
    op.create_index("ix_kogan_template_sku", "kogan_template_au", ["sku"], unique=False)
    op.create_index("ix_kogan_template_sku_unique", "kogan_template_au", ["sku"], unique=False)

    # 3. 表名恢复原状
    op.rename_table("kogan_template_au", "kogan_template")
