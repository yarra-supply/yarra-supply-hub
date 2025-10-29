

# 运费计算使用参数表

from __future__ import annotations
from decimal import Decimal
from sqlalchemy import Integer, Numeric, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base  



"""
   运费/定价计算固定参数（打平列, 仅一行记录）
"""
class FreightCalcConfig(Base):
    
    __tablename__ = "freight_calc_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

     #  Adjust 配置 ---
    adjust_threshold: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=25.0)  
    adjust_rate:      Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.04) 

    # Remote config
    remote_1: Mapped[int] = mapped_column(Integer, nullable=False, default=999)
    remote_2: Mapped[int] = mapped_column(Integer, nullable=False, default=9999)
    wa_r:    Mapped[int] = mapped_column(Integer, nullable=False, default=9999)

     # 权重（WeightedAveS）
    weighted_ave_shipping_weights: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.95)  
    weighted_ave_rural_weights:    Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.05)  
    
    # 体积重
    cubic_factor:   Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=250.0)
    cubic_headroom: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=1.0)

    # ShippingType thresholds
    price_ratio:      Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.3)
    med_dif_10:       Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=10.0)
    med_dif_20:       Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=20.0)
    med_dif_40:       Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=40.0)
    same_shipping_0:  Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.0)
    same_shipping_10: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=10.1)
    same_shipping_20: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=20.1)
    same_shipping_30: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=30.1)
    same_shipping_50: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=50.0)
    same_shipping_100: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=100.0)

    # Shopify
    shopify_threshold:   Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=25.0)
    shopify_config1:  Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=1.26)
    shopify_config2: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=1.22)

    # Kogan AU
    kogan_au_normal_low_denom:  Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.79)
    kogan_au_normal_high_denom: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.82)
    kogan_au_extra5_discount:   Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.969)
    kogan_au_vic_half_factor:   Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.5)

    # K1
    k1_threshold:           Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=66.7)
    k1_discount_multiplier: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.969)
    k1_otherwise_minus:     Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=2.01)

    # Kogan NZ
    kogan_nz_service_no: Mapped[int]   = mapped_column(Integer,        nullable=False, default=9999)
    kogan_nz_config1:    Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.08)
    kogan_nz_config2:    Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.12)
    kogan_nz_config3:    Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.90)


    # Weight（calculate weight 配置）
    # ShippingMed / weight_calc_divisor；± weight_tolerance_ratio 容差内用 max(weight, cubic_weight)
    weight_calc_divisor: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=1.5)
    weight_tolerance_ratio:Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=0.15)

    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
