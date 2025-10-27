
# Engine/Session 工厂 + FastAPI 依赖

from __future__ import annotations
from contextlib import contextmanager
from typing import Generator, Iterator, Optional  #返回一个生成器

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.core.config import settings


# ---- Engine ----
# SQLAlchemy 2.x: create_engine 默认开启 "future" 行为；连接池参数可按需微调
engine = create_engine(
    settings.DATABASE_URL,   # 例如：postgresql+psycopg://user:pass@host:5432/dbname
    pool_size=10,            # 常驻连接
    max_overflow=20,         # 高峰期额外连接
    pool_pre_ping=True,      # 连接失效探测，避免 "MySQL gone away"/"server closed the connection"
    pool_recycle=1800,       # 秒；半小时回收一次，防止长连接被中间设备断开
    echo=False,              # 调试可设为 True
    future=True,
)



# ---- Session Factory ----
# 注意：autocommit=False, autoflush=False 更易控事务与 flush 时机
SessionLocal: sessionmaker[Session] = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,  # 提交后对象仍可用（减少再次查询）
    class_=Session,
    future=True,
)



'''
FastAPI 依赖：为每个请求提供独立会话
用法：
from app.db.session import get_db
def endpoint(db: Session = Depends(get_db)): ...
'''
def get_db() -> Generator[Session, None, None]:
    db: Session = SessionLocal()
    try:
        yield db    # 业务中应在 service/repo 中明确 db.commit()/rollback()；此处不做隐式提交
    finally:
        db.close()  # 归还连接到连接池



# ---- 脚本/任务里的简便上下文管理器（非 FastAPI 场景）目前不用----
@contextmanager
def session_scope() -> Iterator[Session]:

    db: Session = SessionLocal()
    try:
        yield db    # 注意：是否在这里自动 commit 取决于偏好；建议在业务层显式 commit/rollback
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---- 优雅关停：在应用 shutdown 时释放连接池 ----
"""
    释放连接池中的所有连接；在 FastAPI 的 shutdown 钩子中调用。
"""
def dispose_engine() -> None:
    engine.dispose()