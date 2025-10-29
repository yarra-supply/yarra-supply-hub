"""update freight_runs schema to match new orchestration flow

Revision ID: 7ccaf0a51b0f
Revises: 4b892f21ee55
Create Date: 2025-10-29 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7ccaf0a51b0f'
down_revision = '4b892f21ee55'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 新增与流程匹配的字段
    op.add_column('freight_runs', sa.Column('product_run_id', sa.String(length=36), nullable=True))
    op.add_column('freight_runs', sa.Column('candidate_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('freight_runs', sa.Column('changed_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('freight_runs', sa.Column('message', sa.Text(), nullable=True))
    op.add_column('freight_runs', sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True))
    op.create_index('ix_freight_runs_product_run_id', 'freight_runs', ['product_run_id'], unique=False)

    # 清理不再使用的旧字段
    op.drop_column('freight_runs', 'total_batches')
    op.drop_column('freight_runs', 'finished_batches')
    op.drop_column('freight_runs', 'rows_in')
    op.drop_column('freight_runs', 'rows_changed')
    op.drop_column('freight_runs', 'error_summary')

    # 移除 server_default，保持应用侧默认值
    op.alter_column('freight_runs', 'candidate_count', server_default=None, existing_type=sa.Integer())
    op.alter_column('freight_runs', 'changed_count', server_default=None, existing_type=sa.Integer())


def downgrade() -> None:
    # 恢复旧字段
    op.add_column('freight_runs', sa.Column('error_summary', sa.TEXT(), nullable=True))
    op.add_column('freight_runs', sa.Column('rows_changed', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('freight_runs', sa.Column('rows_in', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('freight_runs', sa.Column('finished_batches', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('freight_runs', sa.Column('total_batches', sa.Integer(), nullable=False, server_default='0'))

    op.alter_column('freight_runs', 'total_batches', server_default=None, existing_type=sa.Integer())
    op.alter_column('freight_runs', 'finished_batches', server_default=None, existing_type=sa.Integer())
    op.alter_column('freight_runs', 'rows_in', server_default=None, existing_type=sa.Integer())
    op.alter_column('freight_runs', 'rows_changed', server_default=None, existing_type=sa.Integer())

    op.drop_index('ix_freight_runs_product_run_id', table_name='freight_runs')
    op.drop_column('freight_runs', 'finished_at')
    op.drop_column('freight_runs', 'message')
    op.drop_column('freight_runs', 'changed_count')
    op.drop_column('freight_runs', 'candidate_count')
    op.drop_column('freight_runs', 'product_run_id')
