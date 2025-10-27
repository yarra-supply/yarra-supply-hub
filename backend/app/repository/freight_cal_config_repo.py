

from __future__ import annotations
from typing import Dict, Any
from decimal import Decimal
from sqlalchemy.orm import Session
from app.db.model.freight_cal_config import FreightCalcConfig


# 与模型字段名保持完全一致，防止更新丢字段 / 静默失败
DEFAULTS: Dict[str, Any] = {

    # Adjust 配置 
    "adjust_threshold": 25.0,
    "adjust_rate": 0.04,

    # Remote check
    "remote_1": 999, 
    "remote_2": 9999, 
    "wa_r": 9999,

    # 权重（WeightedAveS）
    "weighted_ave_shipping_weights": 0.95,
    "weighted_ave_rural_weights": 0.05,

    # 体积重
    "cubic_factor": 250.0, 
    "cubic_headroom": 1.0,

    # ShippingType thresholds
    "price_ratio": 0.3,
    "med_dif_10": 10.0,
    "med_dif_20": 20.0,
    "med_dif_40": 40.0,
    "same_shipping_0": 0.0,
    "same_shipping_10": 10.1,
    "same_shipping_20": 20.1,
    "same_shipping_30": 30.1,
    "same_shipping_50": 50.0,
    "same_shipping_100": 100.0,

    # Shopify
    "shopify_threshold": 25.0, 
    "shopify_config1": 1.26, 
    "shopify_config2": 1.22,

    # Kogan AU
    "kogan_au_normal_low_denom": 0.79, 
    "kogan_au_normal_high_denom": 0.82, 
    "kogan_au_extra5_discount": 0.969, 
    "kogan_au_vic_half_factor": 0.5,

    # K1
    "k1_threshold": 66.7, 
    "k1_discount_multiplier": 0.969, 
    "k1_otherwise_minus": 2.01,

    # Kogan NZ
    "kogan_nz_service_no": 9999, 
    "kogan_nz_config1": 0.08, 
    "kogan_nz_config2": 0.12, 
    "kogan_nz_config3": 0.90,

    # Weight（calculate weight）
    "weight_calc_divisor": 1.5,       # ShippingMed / 1.5
    "weight_tolerance_ratio": 0.15,   # 15% 容差
}



# 允许更新的字段白名单（与模型/DEFAULTS 一致
ALL_FIELDS = tuple(DEFAULTS.keys())


def get_or_create_config(db: Session) -> FreightCalcConfig:
    # 查询表中最早的一行；如果已有记录就直接返回
    row = db.query(FreightCalcConfig).order_by(FreightCalcConfig.id.asc()).first()
    if row:
        return row
    
    # 若查询为空，用 DEFAULTS 字典构造一行 FreightCalcConfig，随后 db.add → db.commit → db.refresh，完成写入并返回这条新纪录
    row = FreightCalcConfig(**DEFAULTS)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def to_dict(row: FreightCalcConfig) -> Dict[str, Any]:
    """
    将 ORM 行转为 dict（仅导出受支持字段）。
    """
    def _norm(value: Any) -> Any:
        # SQLAlchemy Numeric -> Decimal；前端需要 number
        if isinstance(value, Decimal):
            return float(value)
        return value

    return {k: _norm(getattr(row, k)) for k in ALL_FIELDS}


def update_config(db: Session, payload: Dict[str, Any]) -> FreightCalcConfig:
    """
    仅更新 payload 中的字段；未提供的保持不变。
    """
    row = get_or_create_config(db)
    for k, v in payload.items():
        if k in ALL_FIELDS:
            setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row



# ------- 查询接口（便于上层直接拿到配置） -------
def get_config_row(db: Session) -> FreightCalcConfig:
    """
    获取 ORM 行（若不存在会自动创建）。
    """
    return get_or_create_config(db)


def get_config_dict(db: Session) -> Dict[str, Any]:
    """
    获取配置的字典形式（字段与 DEFAULTS/模型一致）。
    """
    return to_dict(get_or_create_config(db))
