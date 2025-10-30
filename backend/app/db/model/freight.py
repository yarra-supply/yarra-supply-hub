
from __future__ import annotations
from decimal import Decimal
from typing import Optional
from sqlalchemy import (
    String, Integer, Numeric, Boolean, Text, DateTime, Enum, func, text, Index
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base



# 运费计算结果表
class SkuFreightFee(Base):
    __tablename__ = "kogan_sku_freight_fee"

    sku_code: Mapped[str] = mapped_column(String(64), primary_key=True)  # 唯一 SKU 编码（唯一ID）
    
    adjust:           Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 调整值（price < 25，加4%）
    same_shipping:    Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 同一 SKU 配送不同州最大差值（内部计算）
    shipping_ave:     Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 除 remote 的平均运费（内部计算）

    shipping_ave_m:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 部分州的平均运费（内部计算）
    shipping_ave_r:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 部分州的平均运费（内部计算）
    shipping_med:     Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))      # 运费中位数（内部计算）
    remote_check:     Mapped[bool]            = mapped_column(Boolean, nullable=False, server_default=text("false"))  # 偏远地区不送（9999 运费为 true）

    rural_ave:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # 仅 remote 的平均值（内部计算）
    weighted_ave_s:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # 加权平均运费：ShippingAve*0.95 + RuralAve*0.05
    shipping_med_dif: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # 运费中位数差值：remote - ShippingMed

    weight:           Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # 重新计算weight, 结果用于更新metafields的 + 添加到kogan上传表格上面
    cubic_weight:     Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 3))   # 体积重 (长*宽*高/6000)
    shipping_type:    Mapped[Optional[str]]   = mapped_column(String(24))         # 运费类型（0: FreeShipping, 1: Kogan 平台计算）
    price_ratio:      Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))   # RuralAve / Price 比值

    selling_price:    Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # 售价（有效售价或原价）,有 Special Price 用 Special, 否则用 regular price
    shopify_price:    Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # Shopify 价格（Selling Price 加固定加价）, 根据DSZ配置的shopify规则计算的
    kogan_au_price:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # Kogan AU 最终售价（含运费+GST 等）
    kogan_k1_price:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # Kogan 会员价
    kogan_nz_price:   Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))   # Kogan NZ 售价（可选）

    # —— 幂等&选择性重算 —— 
    # 表示上一次成功完成运费计算时那一刻入参字段的哈希（与 sku_info.attrs_hash_current 对比）成功后回写 last_calc := current
    attrs_hash_last_calc: Mapped[Optional[str]] = mapped_column(String(128))

    # === 给 Kogan 导出用的变化标记 ===
    last_changed_run_id: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)   # 关联freight_run_id 精准取本次产生变化的数据, String(32)，与 FreightRun.id 一致
    last_changed_source: Mapped[str | None] = mapped_column(String(32))         # 'full_sync' | 'price_reset'
    last_changed_at: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)

    # 给导出流程筛选“待导出”的轻量开关
    kogan_dirty_au: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    kogan_dirty_nz: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index(
            "ix_kogan_dirty_au_true_only",
            "sku_code",
            postgresql_where=text("kogan_dirty_au = true")
        ),
        Index(
            "ix_kogan_dirty_nz_true_only",
            "sku_code",
            postgresql_where=text("kogan_dirty_nz = true")
        ),
        Index("ix_kogan_sku_freight_fee_shipping_type", "shipping_type"),
    )




# 运费计算记录表
# 不是必须，也可以先不上线 FreightRun，但有了它，排障/观测会简单很多（比如扫 stuck run、统计变更率等
class FreightRun(Base):
    __tablename__ = "freight_runs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)  

    # 状态机（pending/running/completed/failed/canceled），配合监控与重试
    status: Mapped[str] = mapped_column(
        Enum("pending", "running", "completed", "failed", "canceled",
             name="freight_run_status", create_constraint=True),
        nullable=False, default="pending"
    )

    triggered_by:   Mapped[Optional[str]] = mapped_column(String(32))          # 触发来源（auto/manual/post-5.1 等），用于审计。
    product_run_id: Mapped[Optional[str]] = mapped_column(String(36), index=True)  # 关联 product_sync_runs.id（UUID 字符串）
    candidate_count: Mapped[int] = mapped_column(Integer, default=0)          # 候选 SKU 数
    changed_count:   Mapped[int] = mapped_column(Integer, default=0)          # 实际发生变更的 SKU 数
    message:         Mapped[Optional[str]] = mapped_column(Text)              # 说明或失败信息
    finished_at:     Mapped[Optional[object]] = mapped_column(DateTime(timezone=True))  # 完成时间（成功/失败）

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
