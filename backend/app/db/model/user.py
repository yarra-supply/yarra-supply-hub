
from __future__ import annotations
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base



class User(Base):

    __tablename__ = "users"

    id:       Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, index=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name:       Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    is_active:    Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))   # server_default 使用 SQL
    is_superuser: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))  

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())  
