
from __future__ import annotations
from typing import Optional
from sqlalchemy import String, Boolean, Integer, DateTime, CheckConstraint, func, Index
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


"""
  schedules 表
  - key: 业务键，例如 price_reset / product_full_sync
 """
class Schedule(Base):

    __tablename__ = "schedules"

    # 主键：业务键
    key: Mapped[str]  = mapped_column(String(64), primary_key=True)  # 任务键：price_reset/product_full_sync

    enabled:     Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)  # 是否启用
    day_of_week: Mapped[str] = mapped_column(String(3), nullable=False)            # 'MON'...'SUN'
    hour:        Mapped[int] = mapped_column(Integer, nullable=False)                     # 0..23
    minute:      Mapped[int] = mapped_column(Integer, nullable=False)                   # 0..59
    every_2_weeks: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    timezone:    Mapped[str] = mapped_column(String(64), nullable=False, default="Australia/Sydney")

    # 上次“真正触发成功”的时间（UTC 带时区）
    last_run_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # 打开基础校验，避免脏数据
        CheckConstraint("hour >= 0 AND hour <= 23", name="ck_schedules_hour"),           # CHANGED
        CheckConstraint("minute >= 0 AND minute <= 59", name="ck_schedules_minute"),     # CHANGED
        CheckConstraint("day_of_week IN ('MON','TUE','WED','THU','FRI','SAT','SUN')", name="ck_schedules_dow"),  # CHANGED
        Index("ix_schedules_enabled_time", "enabled", "day_of_week", "hour", "minute"),
    )