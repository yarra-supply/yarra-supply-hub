
from __future__ import annotations
from typing import Optional, Any

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.auth_service import get_current_user
from app.repository.freight_cal_config_repo import (
    get_or_create_config,
    update_config,
    to_dict,
)

router = APIRouter(
    prefix="/freight-config",
    tags=["freight-config"],
    dependencies=[Depends(get_current_user)],
)


class FreightConfig(BaseModel):
    adjust_threshold: float
    adjust_rate: float

    # Remote 哨兵
    remote_1: float
    remote_2: float
    wa_r: float

    # 权重
    weighted_ave_shipping_weights: float
    weighted_ave_rural_weights: float
    # 体积重
    cubic_factor: float
    cubic_headroom: float

    # ShippingType thresholds
    price_ratio: float
    med_dif_10: float
    med_dif_20: float
    med_dif_40: float
    same_shipping_0: float
    same_shipping_10: float
    same_shipping_20: float
    same_shipping_30: float
    same_shipping_50: float
    same_shipping_100: float

    # Shopify
    shopify_threshold: float
    shopify_config1: float
    shopify_config2: float

    # Kogan AU
    kogan_au_normal_low_denom: float
    kogan_au_normal_high_denom: float
    kogan_au_extra5_discount: float
    kogan_au_vic_half_factor: float

    k1_threshold: float
    k1_discount_multiplier: float
    k1_otherwise_minus: float

    # Kogan NZ
    kogan_nz_service_no: float
    kogan_nz_config1: float
    kogan_nz_config2: float
    kogan_nz_config3: float

    weight_calc_divisor: float
    weight_tolerance_ratio: float


class FreightConfigPartial(BaseModel):
    adjust_threshold: Optional[float] = None
    adjust_rate: Optional[float] = None

    remote_1: Optional[float] = None
    remote_2: Optional[float] = None
    wa_r: Optional[float] = None

    weighted_ave_shipping_weights: Optional[float] = None
    weighted_ave_rural_weights: Optional[float] = None

    cubic_factor: Optional[float] = None
    cubic_headroom: Optional[float] = None

    price_ratio: Optional[float] = None
    med_dif_10: Optional[float] = None
    med_dif_20: Optional[float] = None
    med_dif_40: Optional[float] = None
    same_shipping_0: Optional[float] = None
    same_shipping_10: Optional[float] = None
    same_shipping_20: Optional[float] = None
    same_shipping_30: Optional[float] = None
    same_shipping_50: Optional[float] = None
    same_shipping_100: Optional[float] = None

    shopify_threshold: Optional[float] = None
    shopify_config1: Optional[float] = None
    shopify_config2: Optional[float] = None

    kogan_au_normal_low_denom: Optional[float] = None
    kogan_au_normal_high_denom: Optional[float] = None
    kogan_au_extra5_discount: Optional[float] = None
    kogan_au_vic_half_factor: Optional[float] = None

    k1_threshold: Optional[float] = None
    k1_discount_multiplier: Optional[float] = None
    k1_otherwise_minus: Optional[float] = None

    kogan_nz_service_no: Optional[float] = None
    kogan_nz_config1: Optional[float] = None
    kogan_nz_config2: Optional[float] = None
    kogan_nz_config3: Optional[float] = None

    weight_calc_divisor: Optional[float] = None
    weight_tolerance_ratio: Optional[float] = None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("", response_model=FreightConfig)
def get_config(response: Response, db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    row = get_or_create_config(db)
    return _serialize(row)


@router.put("", response_model=FreightConfig)
def put_config(payload: FreightConfig, response: Response, db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    row = update_config(db, payload.model_dump())
    return _serialize(row)


@router.patch("", response_model=FreightConfig)
def patch_config(payload: FreightConfigPartial, response: Response, db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    update_data = payload.model_dump(exclude_none=True)
    if update_data:
        row = update_config(db, update_data)
    else:
        row = get_or_create_config(db)
    return _serialize(row)


def _serialize(row: Any) -> FreightConfig:
    data = to_dict(row)
    return FreightConfig.model_validate(data)
