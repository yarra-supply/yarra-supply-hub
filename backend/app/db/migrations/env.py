
# Alembic 驱动脚本，线上/离线模式都能跑

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig
import logging

from app.core.config import settings
from app.db.base import Base
import app.db.model # 关键：导入所有模型



config = context.config
print("ALEMBIC_INI_LOADED:", config.config_file_name)


# 用 Settings 覆盖连接串（优先于 ini
if settings.DATABASE_URL:
    config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)


# 日志：ini 有 logging 段就用；否则降级 basicConfig，避免 KeyError
try:
    if config.config_file_name:
        fileConfig(config.config_file_name)
    else:
        logging.basicConfig(level=logging.INFO)
except KeyError:
    logging.basicConfig(level=logging.INFO)


# Alembic 的目标元数据（包含所有 ORM 表)
target_metadata = Base.metadata


# --- 可选：过滤对象（例如跳过视图等）。默认不过滤 ---
def include_object(object, name, type_, reflected, compare_to):
    return True


"""在不连接数据库的情况下生成 SQL（离线模式）"""
def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url, 
        target_metadata=target_metadata, 
        literal_binds=True,
        # include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


"""连接数据库直接执行迁移（在线模式）"""
def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.", 
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata,
            # include_object=include_object,  
            render_as_batch=False,           # 如需 batch（SQLite）可调 True
            compare_type=True,               # 比较列类型变化（含 Numeric 精度等）
            compare_server_default=True,     # 比较 server_default 变化
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()