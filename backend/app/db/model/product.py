
from __future__ import annotations
from decimal import Decimal
import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime, date
import uuid

from sqlalchemy import (
    DateTime, String, Integer, UniqueConstraint, CheckConstraint,
    Index, func, text, Numeric, Date, ForeignKey, Text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base



"""
  SKU基础信息表
"""
class SkuInfo(Base):

    __tablename__ = 'sku_info'
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    sku_code:           Mapped[str]           = mapped_column(String(255), unique=True, index=True, nullable=False)  # 唯一 SKU
    shopify_variant_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)  # 来自Shopify变体ID
    
    # 库存和状态
    stock_qty: Mapped[int] = mapped_column(Integer, default=0) 
    # status = Column(Boolean, default=True)  # 是否有库存
    # discontinued = Column(Boolean, default=False) # 不需要
    
    # 价格信息
    price:                  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))    # DSZ原价 = shopify商品页面 的 cost per item
    rrp_price:              Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))    # DSZ直接提供给的建议零售价 = shopify商品页面的 compare-at price
    special_price:          Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))    # DSZ特价
    special_price_end_date: Mapped[Optional[datetime]] = mapped_column(Date)             # DSZ特价结束日期
    shopify_price:          Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))    # Shopify商品页面的 price, 根据dsz配置规则计算出来的
    
    product_tags: Mapped[List[str]]      = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    brand:        Mapped[Optional[str]] = mapped_column(String(255))
    weight:       Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))
    length:       Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))
    width:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))
    height:       Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))
    ean_code:     Mapped[Optional[str]] = mapped_column(String(255))       # = shopify barcode，传到各个平台，直接用
    supplier:     Mapped[Optional[str]] = mapped_column(String(255))

    # 运费相关 17个字段
    freight_act:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_nsw_m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_nsw_r: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_nt_m:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_nt_r:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_qld_m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_qld_r: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    remote:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_sa_m:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_sa_r:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_tas_m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_tas_r: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_vic_m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_vic_r: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_wa_m:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_wa_r:  Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    freight_nz:    Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    # or 存json？
    # freight_by_zone = Column(JSONB, server_default=text("'{}'::jsonb")) # 各州运费，json格式存储

    # 影响运费/定价计算的所有入参字段”的当前快照哈希, 由参与运费/定价的字段按固定顺序拼接计算，如 SHA-256/MD5
    # 把 FREIGHT_RELEVANT_FIELDS 作为“入参字段白名单”，对其按固定顺序序列化后做哈希，得到 attrs_hash_current
    attrs_hash_current: Mapped[str]          = mapped_column(String, nullable=False, default="")

    created_at:      Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)  
    updated_at:      Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False) 
    last_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)  

    __table_args__ = (
        Index("idx_sku_info_last_changed_at", "last_changed_at"),
        Index("idx_sku_info_variant_id", "shopify_variant_id"),
        Index("idx_sku_info_special_end", "special_price_end_date"),
    )




""" 
  商品同步运行记录表 
  商品同步跑(run)”的总账/元数据，用来追踪整次同步的生命周期
"""
class ProductSyncRun(Base):

    __tablename__ = 'product_sync_runs'
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_type: Mapped[Optional[str]] = mapped_column(String(32))                  # 'full_sync' | 'price_reset' | 'incremental'
    status:   Mapped[str]           = mapped_column(String, default="running")   # 运行状态: running/completed/failed

    # Shopify 信息关联
    shopify_bulk_id:     Mapped[Optional[str]] = mapped_column(String(64))  # Shopify Bulk 的操作 ID
    shopify_bulk_status: Mapped[Optional[str]] = mapped_column(String(32))  # Bulk 状态（SUBMITTED/COMPLETED/FAILED…）
    shopify_bulk_url:    Mapped[Optional[str]] = mapped_column(Text)        # JSONL 下载 URL

    # 规模与产出
    total_shopify_skus: Mapped[Optional[int]] = mapped_column(Integer)  # 由 Shopify objectCount 或解析后得到
    changed_count:      Mapped[Optional[int]] = mapped_column(Integer)  # 所有分片累计发生变化的 SKU 数
    note:               Mapped[Optional[str]] = mapped_column(String)   # 备注（失败原因、手工触发人等）
    
    # 时间追踪
    started_at:           Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at:          Mapped[object | None] = mapped_column(DateTime(timezone=True))
    webhook_received_at:  Mapped[object | None] = mapped_column(DateTime(timezone=True))  # 收到 bulk finish webhook 的时刻（可选）

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (Index("idx_sync_run_status", "status", "created_at"),)




'''
  变更候选明细表
  记录“本次 run 中字段有变化的 SKU”及其旧值/新值（仅变更字段）
  使用场景: 运费计算使用, 只处理这张表里的sku
'''
class ProductSyncCandidate(Base):

    __tablename__ = "product_sync_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        ForeignKey("product_sync_runs.id",  ondelete="CASCADE"),
        index=True, 
        nullable=False,
    )
    
    sku_code: Mapped[str]     = mapped_column(String(64), index=True, nullable=False)
    # 变化字段集合，如 {'price': true, 'weight': true}
    change_mask: Mapped[Dict[str, bool]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    # 仅“变化字段”的新值子集
    new_snapshot: Mapped[Dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"), 
    )
    change_count: Mapped[int] = mapped_column(
        Integer,
        server_default=text("0"),
        nullable=False,
    )

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)  # 最后一次写入时间

    __table_args__ = (
        UniqueConstraint("run_id", "sku_code", name="ux_psc_run_sku"),
        CheckConstraint(
            "(jsonb_typeof(new_snapshot) = 'object') AND (change_count > 0)",
            name="ck_psc_non_empty",
        ),
        # 按 run + 时间倒序拉取
        Index("ix_psc_run_created_desc", "run_id", text("created_at DESC")),

        # GIN(change_mask)，默认 opclass jsonb_ops 即可满足 `?` / `@>` 查询
        Index("gin_psc_change_mask", "change_mask", postgresql_using="gin"),

        # LOWER(sku_code) 便于大小写不敏感检索（可选）
        Index("ix_psc_lower_sku", text("LOWER(sku_code)")),
    )



# ===================== 分片清单（Manifest）===================== #
# 记录每个分片的“内容与执行状态”，支持按编号/状态重投
class ProductSyncChunk(Base): 

    __tablename__ = "product_sync_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("product_sync_runs.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    chunk_idx: Mapped[int] = mapped_column(Integer, nullable=False)   # 分片编号（0..N-1）
    status:    Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'pending'"))  # pending/running/succeeded/failed

    # 本片的内容（直接保存 (sku, variant_id) 二元组数组，便于精准重投）
    sku_codes: Mapped[List[str]] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))  # CHANGED: List[str]
    sku_count: Mapped[int]       = mapped_column(Integer, nullable=False, server_default=text("0"))

    # 便于页面观察
    # first_sku: Mapped[Optional[str]] = mapped_column(String(64))
    # last_sku:  Mapped[Optional[str]] = mapped_column(String(64))

    # DSZ 健康度指标（分片级）
    dsz_missing:         Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    dsz_failed_batches:  Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    dsz_failed_skus:     Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    dsz_requested_total: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    dsz_returned_total:  Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    # DSZ 健康度（明细列表，JSONB 数组；限长由上层控制）
    dsz_missing_sku_list: Mapped[List[str]] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))  
    dsz_failed_sku_list:  Mapped[List[str]] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))  
    dsz_extra_sku_list:   Mapped[List[str]] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))  

    # 执行时间与错误
    started_at:  Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_error:  Mapped[Optional[str]]      = mapped_column(Text)

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("run_id", "chunk_idx", name="ux_pschunk_run_idx"),  # 幂等写入
        Index("ix_pschunk_run_status_idx", "run_id", "status", "chunk_idx"),
        Index("ix_pschunk_run_idx", "run_id", "chunk_idx"),
    )


