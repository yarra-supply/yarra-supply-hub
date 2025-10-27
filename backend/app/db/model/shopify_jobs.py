
# app/db/model/shopify_jobs.py
from sqlalchemy import Column, String, DateTime, JSON, Integer, func
from app.db.base import Base


class ShopifyUpdateJob(Base):
    __tablename__ = "shopify_update_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shop_id = Column(String(32), nullable=True, default="main")
    sku_code = Column(String(64), nullable=False)
    
    op = Column(String(32), nullable=False)       # "metafieldsSet" / "productVariantUpdate" ...
    payload = Column(JSON, nullable=False)
    status = Column(String(16), nullable=False, default="pending")  # pending/processing/succeeded/failed
    available_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        # 避免重复插相同操作（最小去重；必要时可改为更细维度的唯一键）
        # 注意：同一 SKU 多字段已在 payload 合并
        {'sqlite_autoincrement': True},
    )
