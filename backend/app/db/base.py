

# 统一的 ORM 基类 + 命名规范

from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import MetaData

#  统一命名规范，Alembic 迁移时约束/索引名字稳定可预期
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


# SQLAlchemy ORM 的“基类工厂”。
# 所有的表模型（SkuInfo, ProductSyncRun, ProductSyncCandidate …）都要 继承 这个 Base 
# 才能被 ORM 识别、映射到数据库表。
class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)