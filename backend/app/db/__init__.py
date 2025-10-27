
# 导出入口，给脚本/临时建表用

from .session import engine, SessionLocal, get_db, dispose_engine
from app.db.model import *  # 确保把所有模型加载进 Base.metadata
from .base import Base


# 仅开发期/临时使用；生产请统一用 Alembic 迁移
"""
    开发期在空库快速建表：
        python -c "from app.db import create_all; create_all()"
    生产环境禁用；请使用 `alembic upgrade head`
"""
def create_all() -> None:
    Base.metadata.create_all(bind=engine)

