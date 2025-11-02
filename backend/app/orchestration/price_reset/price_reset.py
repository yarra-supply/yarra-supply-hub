
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Iterator, List, Tuple, Dict, Any, Optional

from zoneinfo import ZoneInfo
from celery import shared_task
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.core.config import settings

from app.repository.product_repo import iter_price_reset_candidates, load_state_freight_by_skus
from app.repository.freight_repo import load_fee_rows_by_skus, update_changed_prices
from app.services.freight.freight_compute import (
    compute_all, FreightInputs as FCInputs,
)


logger = logging.getLogger(__name__)

# 可调参数（含默认值）
_DB_PAGE_SIZE = int(getattr(settings, "PRICE_RESET_DB_PAGE", 1000))          # DB 分页
_UPSERT_CHUNK = int(getattr(settings, "PRICE_RESET_UPSERT_CHUNK", 500))      # 每批 upsert 的行数
_CELERY_TZ = ZoneInfo(getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne"))

# ---- 业务字段名（结果表需要维护的列）----
_OUTPUT_FIELDS = (
    ("adjust", "adjust"),
    ("same_shipping", "same_shipping"),
    ("shipping_ave", "shipping_ave"),
    ("shipping_ave_m", "shipping_ave_m"),
    ("shipping_ave_r", "shipping_ave_r"),
    ("shipping_med", "shipping_med"),
    ("remote_check", "remote_check"),
    ("rural_ave", "rural_ave"),
    ("weighted_ave_s", "weighted_ave_s"),
    ("shipping_med_dif", "shipping_med_dif"),
    ("weight", "weight"),
    ("cubic_weight", "cubic_weight"),
    ("shipping_type", "shipping_type"),
    ("price_ratio", "price_ratio"),
    ("selling_price", "selling_price"),
    ("shopify_price", "shopify_price"),
    ("kogan_au_price", "kogan_au_price"),
    ("kogan_k1_price", "kogan_k1_price"),
    ("kogan_nz_price", "kogan_nz_price"),
)
_TARGET_COLS = tuple(name for name, _ in _OUTPUT_FIELDS)

# 简单且足够的“本次运行”ID（本地时区 epoch-ms）
def _gen_run_id() -> int:
    return int(datetime.now(_CELERY_TZ).timestamp() * 1000)



"""
入口：由 scheduler_tick.tick_price_reset() 触发
    1) 计算“明天”本地日期
    2) 从 DB 流式拉取 special_price_end_date <= 明天日期的 所有 Sku_code + price + special_price list （候选 ~3k)
    3) 获取这部分sku对应的 kogan_sku_freight_fee表的数据
    4) set selling_price = price, 再用selling price 重新计算 kogan_au_price, kogan_nz_price 使用freight_compute.py里面相同的公式
    5) 更新kogan_sku_freight_fee表的 selling price, kogan_au_price, kogan_nz_price, kogan_k1_price 字段
"""
@shared_task(name="app.orchestration.price_reset.price_reset.kick_price_reset")
def kick_price_reset() -> dict:

    target_date = _tomorrow_local_date() # 明天（本地时区）
    db: Session = SessionLocal()
    processed, changed_rows = 0, 0
    run_id = _gen_run_id()  # 生成一个这次流程的 run_id

    try:
        batch: List[str] = []
        to_update = []   # (sku, {col->new})


        # 2) 流式拉取候选 sku_code 列表
        for record in iter_price_reset_candidates(db, target_date=target_date, page_size=_DB_PAGE_SIZE):
            sku = record[0] if isinstance(record, (list, tuple)) else record
            sku = str(sku)
            batch.append(sku)

            if len(batch) >= _UPSERT_CHUNK:
                # 批量重新计算所有price，更新to_update
                _process_batch(db, batch, to_update)
                processed += len(batch)
                batch.clear()

                if to_update:
                    # 只更新变化列
                    update_changed_prices(db, to_update, source="price_reset", run_id=run_id)
                    db.commit()
                    changed_rows += len(to_update)
                    to_update.clear()
        
        # 收尾
        if batch:
            # 批量重新计算所有price，更新to_update
            _process_batch(db, batch, to_update)
            processed += len(batch)
            batch.clear()

        if to_update:
            # 只更新变化列, 更新后的数据等待 download触发新kogan_template 模版计算
            update_changed_prices(db, to_update, source="price_reset", run_id=run_id)
            db.commit()
            changed_rows += len(to_update)
            to_update.clear()
        
        logger.info("price_reset done date=%s processed=%d changed=%d", target_date, processed, changed_rows)
        return {"date": str(target_date), "processed": processed, "changed": changed_rows}
    
    except Exception as e:
        logger.exception("price_reset failed date=%s", target_date)
        db.rollback()
        return {"date": str(target_date), "processed": processed, "changed": changed_rows, "error": str(e)}
    finally:
        db.close()




# ===================== 批处理：装配输入→计算→与旧值对比→收集变化 =====================
def _process_batch(
    db: Session,
    batch: List[str],
    to_update: List[Tuple[str, Dict[str, object]]],
):
    skus = list(batch)
    if not skus:
        return

    # 1) 查询kogan_sku_freight_fee，获取这批 SKU 对应的 运费表旧值
    old_price_map: Dict[str, Dict[str, object]] = load_fee_rows_by_skus(db, skus)

    # 2) 查询sku_info, 拉各州运费/重量等，供 compute_all 使用
    product_freight_snap = load_state_freight_by_skus(db, skus)

    # 3) by sku 处理
    for sku in skus:
        one_product = product_freight_snap.get(sku) or {}
        one_old_price = old_price_map.get(sku) or {}

        if not one_product:
            logger.warning("skip price_reset sku=%s due to missing sku_info snapshot", sku)
            continue

        price = _as_float(one_product.get("price"))
        if price is None:
            logger.warning("skip price_reset sku=%s due to missing price", sku)
            continue

        # 重新计算价格：强制 selling = price
        inputs = FCInputs(
            price=price,

            # core logic: 忽略 special, 强制还原
            special_price=None,
            special_price_end_date=one_product.get("special_price_end_date"),
            # length=_as_float(one_product.get("length")),
            # width=_as_float(one_product.get("width")),
            # height=_as_float(one_product.get("height")),

            weight=_as_float(one_product.get("weight")),
            cbm=_as_float(one_product.get("cbm")),
            
            act=_as_float(one_product.get("freight_act")),
            nsw_m=_as_float(one_product.get("freight_nsw_m")),
            nsw_r=_as_float(one_product.get("freight_nsw_r")),
            qld_m=_as_float(one_product.get("freight_qld_m")),
            qld_r=_as_float(one_product.get("freight_qld_r")),
            sa_m=_as_float(one_product.get("freight_sa_m")),
            sa_r=_as_float(one_product.get("freight_sa_r")),
            tas_m=_as_float(one_product.get("freight_tas_m")),
            tas_r=_as_float(one_product.get("freight_tas_r")),
            vic_m=_as_float(one_product.get("freight_vic_m")),
            vic_r=_as_float(one_product.get("freight_vic_r")),
            wa_m=_as_float(one_product.get("freight_wa_m")),
            wa_r=_as_float(one_product.get("freight_wa_r")),
            nt_m=_as_float(one_product.get("freight_nt_m")),
            nt_r=_as_float(one_product.get("freight_nt_r")),
            remote=_as_float(one_product.get("remote")),
            nz=_as_float(one_product.get("freight_nz")),
        )
        out = compute_all(inputs)

        # 新值（按字段规范化后做对比/写回）
        new_vals: Dict[str, object] = {}
        for col, attr in _OUTPUT_FIELDS:
            new_vals[col] = _normalize_value(col, getattr(out, attr, None))

        # 只挑变化列
        diffs: Dict[str, object] = {}
        for col in _TARGET_COLS:
            old_v = _normalize_value(col, one_old_price.get(col))
            if new_vals[col] != old_v:
                diffs[col] = new_vals[col]

        if diffs:
            to_update.append((sku, diffs))





# ----------------- 帮助函数：算出“明天”的本地日期（字符串 & date） -----------------
"""
使用业务时区（settings.celery_timezone）计算“明天”的日期。
例如 Melbourne 周三 20:00 跑，则 tomorrow=周四的日期。
"""
def _tomorrow_local_date():
    return (datetime.now(_CELERY_TZ).date() + timedelta(days=1))


def _as_float(x) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None
    

def _q2(val) -> Optional[Decimal]:
    """对比与写回前统一保留两位小数，避免浮点误差造成‘假变化’"""
    if val is None:
        return None
    return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _q3(val) -> Optional[Decimal]:
    if val is None:
        return None
    return Decimal(val).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


def _q4(val) -> Optional[Decimal]:
    if val is None:
        return None
    return Decimal(val).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _normalize_value(column: str, val):
    if val is None:
        return None
    if column == "remote_check":
        return bool(val)
    if column == "shipping_type":
        return str(val)
    if column == "price_ratio":
        return _q4(val)
    if column == "cubic_weight":
        return _q3(val)
    return _q2(val)
