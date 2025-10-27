
from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import String, Integer, Numeric, Text, func, Index, Enum as SAEnum, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import sqltypes as _t

from app.db.base import Base


# 国家类型：AU / NZ
CountryType = SAEnum("AU", "NZ", name="country_type_enum")



"""
    表：kogan_template
    来源：Kogan AU Offer Override Template.csv

    说明：
    - CSV 表头中的列名做了 snake_case 处理。
    - CSV 中出现了两个 "SKU" 列；第二个命名为 sku2（你知道其语义后可重命名）。
    - 金额、重量等采用 Numeric，描述类用 Text，其他默认 String。
"""
class KoganTemplate(Base):
    
    __tablename__ = "kogan_template"

    # 通用字段（如已有基类时间戳，可去掉这里的两个字段）
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # ====== CSV 字段 ======
    sku: Mapped[str] = mapped_column(String(128), index=True, nullable=False)  

    price:             Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  # = kogan au price
    rrp:               Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  
    kogan_first_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  

    handling_days: Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    barcode:       Mapped[Optional[str]]  = mapped_column(String(128), nullable=True)
    stock:         Mapped[Optional[int]]  = mapped_column(Integer, nullable=True)
    shipping:      Mapped[Optional[str]]  = mapped_column(String(128), nullable=True)

    weight:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3), nullable=True)
    brand:         Mapped[Optional[str]]     = mapped_column(String(128), nullable=True)
    title:         Mapped[Optional[str]]     = mapped_column(String(512), nullable=True)
    description:   Mapped[Optional[str]]     = mapped_column(Text, nullable=True)
    subtitle:      Mapped[Optional[str]]     = mapped_column(String(512), nullable=True)
    whats_in_the_box: Mapped[Optional[str]]  = mapped_column(Text, nullable=True)

    sku2:        Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    category:    Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    country_type: Mapped[str] = mapped_column(CountryType, nullable=False, server_default="AU")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    

    __table_args__ = (
        Index("ix_kogan_template_sku_unique", "sku", unique=False),
    )
