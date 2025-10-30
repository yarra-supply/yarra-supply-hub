"""add kogan export job tables

Revision ID: f3c1c8c0a123
Revises: e2b1a4c5d6f7
Create Date: 2025-11-01 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'f3c1c8c0a123'
down_revision = 'e2b1a4c5d6f7'
branch_labels = None
depends_on = None


STATUS_ENUM_NAME = 'kogan_export_job_status'
STATUS_VALUES = ('pending', 'exported', 'failed', 'applied', 'apply_failed')


def _ensure_status_enum_exists(bind):
    values_sql = ", ".join(f"'{v}'" for v in STATUS_VALUES)
    bind.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_type WHERE typname = '{STATUS_ENUM_NAME}'
                ) THEN
                    CREATE TYPE {STATUS_ENUM_NAME} AS ENUM ({values_sql});
                END IF;
            END
            $$;
            """
        )
    )


def upgrade() -> None:
    bind = op.get_bind()
    _ensure_status_enum_exists(bind)

    country_enum = postgresql.ENUM('AU', 'NZ', name='country_type_enum', create_type=False)
    status_enum = postgresql.ENUM(*STATUS_VALUES, name=STATUS_ENUM_NAME, create_type=False)

    op.create_table(
        'kogan_export_jobs',
        sa.Column('id', sa.String(length=64), primary_key=True, nullable=False),
        sa.Column('country_type', country_enum, nullable=False),
        sa.Column('status', status_enum, nullable=False, server_default=STATUS_VALUES[0]),
        sa.Column('file_name', sa.String(length=255), nullable=False),
        sa.Column('file_size', sa.Integer(), nullable=False),
        sa.Column('row_count', sa.Integer(), nullable=False, server_default=sa.text('0')),
        sa.Column('file_content', sa.LargeBinary(), nullable=False),
        sa.Column('created_by', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('applied_by', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('exported_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('applied_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('note', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
    )

    op.create_table(
        'kogan_export_job_skus',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('job_id', sa.String(length=64), sa.ForeignKey('kogan_export_jobs.id', ondelete='CASCADE'), nullable=False),
        sa.Column('sku', sa.String(length=128), nullable=False),
        sa.Column('template_payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('changed_columns', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.UniqueConstraint('job_id', 'sku', name='ux_kogan_export_job_sku_unique'),
    )
    op.create_index('ix_kogan_export_job_skus_job_id', 'kogan_export_job_skus', ['job_id'])
    op.create_index('ix_kogan_export_job_skus_sku', 'kogan_export_job_skus', ['sku'])


def downgrade() -> None:
    op.drop_constraint('ux_kogan_export_job_sku_unique', 'kogan_export_job_skus', type_='unique')
    op.drop_index('ix_kogan_export_job_skus_sku', table_name='kogan_export_job_skus')
    op.drop_index('ix_kogan_export_job_skus_job_id', table_name='kogan_export_job_skus')
    op.drop_table('kogan_export_job_skus')
    op.drop_table('kogan_export_jobs')

    bind = op.get_bind()
    bind.execute(sa.text(f"DROP TYPE IF EXISTS {STATUS_ENUM_NAME}"))
