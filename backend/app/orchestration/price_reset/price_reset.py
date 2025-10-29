
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

from app.db.model.product import SkuInfo
from app.repository.product_repo import (
    iter_price_reset_candidates, load_existing_by_skus
)
from app.repository.product_repo import iter_price_reset_candidates, load_state_freight_by_skus
from app.repository.freight_repo import load_fee_rows_by_skus, update_changed_prices
from app.services.freight.freight_compute import (
    compute_all, FreightInputs as FCInputs,
)


logger = logging.getLogger(__name__)

# 可调参数（含默认值）
_DB_PAGE_SIZE = int(getattr(settings, "PRICE_RESET_DB_PAGE", 1000))          # DB 分页
_UPSERT_CHUNK = int(getattr(settings, "PRICE_RESET_UPSERT_CHUNK", 500))      # 每批 upsert 的行数
_CELERY_TZ = getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne")
# ---- 业务字段名（结果表需要维护的 4 列）----
_TARGET_COLS = ("selling_price", "kogan_au_price", "kogan_k1_price", "kogan_nz_price")

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
        batch = []       # (sku, price, special)
        to_update = []   # (sku, {col->new})


        # 2) 流式拉取候选 (sku_code, price, special_price)
        for sku, price, special in iter_price_reset_candidates(db, target_date=target_date, page_size=_DB_PAGE_SIZE):
            batch.append((sku, price, special))

            if len(batch) >= _UPSERT_CHUNK:
                # 批量重新计算kogan au price
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
            # 批量重新计算kogan au price
            _process_batch(db, batch, to_update)
            processed += len(batch)
            batch.clear()

        if to_update:
            # 只更新变化列
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
    batch: List[Tuple[str, float, Optional[float]]],
    to_update:  List[Tuple[str, Dict[str, object]]],
):
    skus = [s for (s, _, _) in batch]

    # 1) 拉取这批 SKU 对应的 运费表旧值
    old_price_map: Dict[str, Dict[str, object]] = load_fee_rows_by_skus(db, skus)

    # 2) 拉各州运费/重量等，供 compute_all 使用
    product_freight_snap = load_state_freight_by_skus(db, skus)

    # 3) by sku 处理
    for sku, price, special_price in batch:
        one_product = product_freight_snap.get(sku) or {}
        one_old_price = old_price_map.get(sku) or {}

        # 重新计算价格：强制 selling = price
        inputs = FCInputs(
            price=price,
            special_price=None,  # 忽略 special，强制还原
            act=one_product.get("freight_act"),
            nsw_m=one_product.get("freight_nsw_m"),
            nsw_r=one_product.get("freight_nsw_r"),
            qld_m=one_product.get("freight_qld_m"),
            qld_r=one_product.get("freight_qld_r"),
            sa_m=one_product.get("freight_sa_m"),
            sa_r=one_product.get("freight_sa_r"),
            tas_m=one_product.get("freight_tas_m"),
            tas_r=one_product.get("freight_tas_r"),
            vic_m=one_product.get("freight_vic_m"),
            vic_r=one_product.get("freight_vic_r"),
            wa_m=one_product.get("freight_wa_m"),
            wa_r=one_product.get("freight_wa_r"),
            nt_m=one_product.get("freight_nt_m"),
            nt_r=one_product.get("freight_nt_r"),
            remote=one_product.get("remote"),
            nz=one_product.get("freight_nz"),
            weight=_as_float(one_product.get("weight")),
            cbm=None,  # 如需体积重可在此补充
        )
        out = compute_all(inputs)

        # 新值（保留两位小数做对比/写回）
        new_vals = {
            "selling_price": _q2(out.selling_price),
            "kogan_au_price": _q2(out.kogan_au_price),
            "kogan_k1_price": _q2(out.kogan_k1_price),
            "kogan_nz_price": _q2(out.kogan_nz_price),
        }

        # 只挑变化列
        diffs: Dict[str, object] = {}
        for col in _TARGET_COLS:
            old_v = _q2(one_old_price.get(col))
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

