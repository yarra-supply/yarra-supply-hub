

from __future__ import annotations
from typing import Optional, Dict, Any
from sqlalchemy import Integer, String, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


'''
运费结果计算参数配置表: 根据公式/规则改变计算结果

用“平台/国家/命名空间”划分的一张规则表：
    - platform: 'global' | 'shipping_type' | 'shopify' | 'kogan' 等
    - country : 'AU'|'NZ' 或 NULL(global 就可为 NULL)
    - namespace: 用于进一步区分同一平台下的子域(可选，例如 'AU' 的多个 profile)
    - rules: 具体参数(JSONB)
'''
class PricingRule(Base):

    __tablename__ = "pricing_rules"

    id:        Mapped[int]                 = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform:  Mapped[str]                 = mapped_column(String(32), nullable=False)   # global / shipping_type / shopify / kogan
    country:   Mapped[Optional[str]]       = mapped_column(String(8))               # AU / NZ / NULL
    namespace: Mapped[Optional[str]]       = mapped_column(String(32))              # 可用于 A/B 或不同店铺，没用到可留空
    rules:     Mapped[Dict[str, Any]]      = mapped_column(JSONB, nullable=False)   # 参数 JSON

    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
