
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.services.freight.freight_compute import FreightInputs, FreightOutputs, compute_all
import json

from app.repository.freight_repo import (
    load_inputs_for_skus,            # -> List[tuple[str, FreightInputs]]
    query_existing_results_map,      # -> Dict[sku, existing_row]
    upsert_freight_results,          # -> (inserted_count, updated_count)
)
# from app.repository.shopify_repo import enqueue_shopify_jobs;


# 结果表要写入的列名（和 SkuFreightFee 对应）
_RESULT_COLS = [
    "sku_code",
    "adjust",
    "same_shipping",
    "shipping_ave",
    "shipping_ave_m",
    "shipping_ave_r",
    "shipping_med",
    "remote_check",
    "rural_ave",
    "weighted_ave_s",
    "shipping_med_dif",
    "weight",
    "cubic_weight",
    "price_ratio",
    "shipping_type",
    "selling_price",
    "shopify_price",
    "kogan_au_price",
    "kogan_k1_price",
    "kogan_nz_price",
    "attrs_hash_last_calc",
]



"""
把一次“运费计算的输出结果(FreightOutputs)”整理成一行可直接 upsert 到 kogan_sku_freight_fee 的“字典”
   - 把计算得到的业务指标（如 shipping_ave / shipping_med / cubic_weight / …）放进一行里
   - 顺手写入幂等指纹：attrs_hash_last_calc = attrs_hash_current（也就是本次计算时用到的输入侧哈希
"""
def _map_outputs_to_row(sku: str, out: FreightOutputs, attrs_hash_current: Optional[str]) -> Dict[str, Any]:
    row = {
        "sku_code": sku,
        "adjust": out.adjust,
        "same_shipping": out.same_shipping,
        "shipping_ave": out.shipping_ave,
        "shipping_ave_m": out.shipping_ave_m,
        "shipping_ave_r": out.shipping_ave_r,
        "shipping_med": out.shipping_med,
        "remote_check": out.remote_check,
        "rural_ave": getattr(out, "rural_ave", None),
        "weighted_ave_s": getattr(out, "weighted_ave_s", None),
        "shipping_med_dif": getattr(out, "shipping_med_dif", None),
        "cubic_weight": out.cubic_weight,
        "weight": getattr(out, "weight", None),
        "price_ratio": getattr(out, "price_ratio", None),
        "shipping_type": getattr(out, "shipping_type", None),
        "selling_price": getattr(out, "selling_price", None),
        "shopify_price": getattr(out, "shopify_price", None),
        "kogan_au_price": getattr(out, "kogan_au_price", None),
        "kogan_k1_price": getattr(out, "kogan_k1_price", None),
        "kogan_nz_price": getattr(out, "kogan_nz_price", None),

         # 幂等关键：成功后把 current 写到 last_calc
        "attrs_hash_last_calc": attrs_hash_current, 
    }
    return row




# ---------- 主流程：装配输入 -> compute_all -> 对比差异 -> upsert 结果 -> 生成 Shopify 作业 ----------
"""
对一批 SKU：
    1) 组装 FreightInputs 并计算；
    2) 与历史结果比对，只有变化才 upsert；
    3) 仅针对需要的字段生成最小 Shopify 作业（metafieldsSet）。
    返回：(变更条数, 生成作业数)(changed_count, job_count)
"""
def process_batch_compute_and_persist(
    db: Session,
    skus: List[str],
    freight_run_id: str,
    cfg: Optional[Dict[str, Any]] = None,
    trigger: str = "manual"
) -> int:

    if not skus:
        return 0, 0

    # 1) 查询sku_info获取商品数据
    inputs: List[Tuple[str, FreightInputs]] = load_inputs_for_skus(db, skus)

    # 2) 查询运费计算历史结果（用于对比差异）
    old_map = query_existing_results_map(db, skus)  # {sku: SkuFreightFee ORM}

    to_upsert: List[Dict[str, Any]] = []


    # 3) 逐个计算并对比
    for sku, fin in inputs:

        # 运费计算
        out: FreightOutputs = compute_all(fin, cfg=cfg, sku_code=sku)
        
        old = old_map.get(sku)

        # 获取sku_info的 attrs_hash_current, set到 freight row 的 attrs_hash_last_calc 字段
        attrs_hash_current = getattr(fin, "attrs_hash_current", None)

        # fail-fast：没有 attrs_hash_current 直接报错
        # if not attrs_hash_current:
        #     raise ValueError(f"missing attrs_hash_current for sku={sku}")
        row = _map_outputs_to_row(sku, out, attrs_hash_current)

        changed_fields = _RESULT_COLS[:] if old is None else _diff_result(old, row)
        # print(f"CHANGED_FIELDS (sku={sku}): {changed_fields}")

        if changed_fields:
            #  Kogan 导出标记 
            row["last_changed_run_id"] = freight_run_id
            row["last_changed_source"] = trigger
            row["last_changed_at"] = datetime.now(timezone.utc)
            row["kogan_dirty_au"] = True
            row["kogan_dirty_nz"] = True
            to_upsert.append(row)

    # 4) 落库 upsert
    inserted, updated = (0, 0)
    if to_upsert:
        inserted, updated = upsert_freight_results(db, to_upsert)

    # 5) todo 生成 Shopify 作业
    changed_total = inserted + updated
    return changed_total




"""
返回 old 与 new_row 之间发生变化的字段名列表（用于决定是否 upsert & 生成作业）。
对比变更字段，返回变更列名列表（简化实现：只要字段存在且值不同就算变更）。
old_row 是 ORM 对象或具备同名属性的对象。
"""
def _diff_result(old_row: Any, new_row: Dict[str, Any]) -> List[str]:
    changed = []
    for col in _RESULT_COLS:
        if not hasattr(old_row, col):
            # 旧表可能没有某些列（兼容性处理），跳过
            continue
        if getattr(old_row, col) != new_row.get(col):
            changed.append(col)
    return changed



"""
构建shopify metafields更新字段
"""
def _build_metafields_payload(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    
    # 如果只有 shipping_ave 变化，就下发一个对应 metafield
    if row.get("shipping_ave") is None:
        return None
    
    return {
        "sku": row["sku_code"],
        "metafields": [
            {"namespace": "yarra", "key": "shipping_ave", "type": "number_decimal", "value": str(row["shipping_ave"])},
        ]
    }
