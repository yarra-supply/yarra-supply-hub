
# 运费计算结果相关的 DB 操作

from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.model.product import SkuInfo, ProductSyncCandidate   # 商品信息表
from app.db.model.freight import SkuFreightFee, FreightRun
# from app.db.model.shopify_jobs import ShopifyUpdateJob           # 待派发到同步shopify的作业表

from app.utils.attrs_hash import FREIGHT_HASH_FIELDS             # 作为“运费相关字段集合
FREIGHT_RELEVANT_FIELDS: set[str] = set(FREIGHT_HASH_FIELDS)



"""
轻量输入结构：与 app.services.freight_compute 里的 FreightInputs 字段保持一致。
这里不做运算，仅承载数据（为了避免 repo 依赖 service）。
"""
class FreightInputs:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)



"""
feature: 候选 SKU 读取
从 product_sync_candidates 读取“与运费计算相关字段有变化”的 sku 列表（去重）。
   - change_mask 若是数组：直接集合相交；若是 JSONB：改成 contains/has_any 判断即可。
   - 候选 SKU：根据本次 product run 的变更字段过滤
"""
def get_candidate_skus_from_run(db: Session, product_run_id: str) -> List[str]:
    rows = db.execute(
        select(ProductSyncCandidate.sku_code, ProductSyncCandidate.change_mask)
        .where(ProductSyncCandidate.run_id == product_run_id)
    ).all()

    skus: List[str] = []

    # 若 change_mask 和 FREIGHT_HASH_FIELDS 有交集，则将 sku_code 纳入
    for sku, mask in rows:
        # change_mask 形如 ["price","freight_vic_m", ...]
        if not mask:
            continue
        if set(mask) & FREIGHT_RELEVANT_FIELDS:
            skus.append(sku)

    # 去重保持顺序
    return list(dict.fromkeys(skus))



"""
feature: 载入计算输入
从 sku_info 构建 FreightInputs：
   - 价格优先字段：promotion_price/special_price -> price
   - 各州运费：映射为 ACT/NSW_M/.../REMOTE/NZ
   - 重量/体积：weight_kg / cbm（若只有 weight 字段也兼容）
"""
def load_inputs_for_skus(db: Session, skus: List[str]) -> List[Tuple[str, FreightInputs]]:

    if not skus:
        return []
    rows = (
        db.query(SkuInfo)
        .filter(SkuInfo.sku_code.in_(skus))
        .all()
    )
    ret: List[Tuple[str, FreightInputs]] = []

    # 从 SkuInfo 读出一批 SKU 的属性，组装成 (sku_code, FreightInputs)
    for r in rows:
        fi = FreightInputs(

            # 这里按 compute_all 需要的字段进行装配: # 基础价格/尺寸/重量/哈希
            price=getattr(r, "price", None),
            special_price=getattr(r, "special_price", None),
            special_price_end_date=getattr(r, "special_price_end_date", None),
            length=getattr(r, "length", None),
            width=getattr(r, "width", None),
            height=getattr(r, "height", None),
            weight=getattr(r, "weight", None),
            cbm=getattr(r, "cbm", None),

            attrs_hash_current=getattr(r, "attrs_hash_current", None),

            # 各州运费输入（从 SkuInfo.freight_* 读取）: compute_all 需要的字段
            act =   getattr(r, "freight_act", None),      
            nsw_m = getattr(r, "freight_nsw_m", None),    
            nsw_r = getattr(r, "freight_nsw_r", None),    
            nt_m  = getattr(r, "freight_nt_m", None),     
            nt_r  = getattr(r, "freight_nt_r", None),     
            qld_m = getattr(r, "freight_qld_m", None),    
            qld_r = getattr(r, "freight_qld_r", None),    
            remote= getattr(r, "remote", None),           
            sa_m  = getattr(r, "freight_sa_m", None),     
            sa_r  = getattr(r, "freight_sa_r", None),     
            tas_m = getattr(r, "freight_tas_m", None),    
            tas_r = getattr(r, "freight_tas_r", None),    
            vic_m = getattr(r, "freight_vic_m", None),   
            vic_r = getattr(r, "freight_vic_r", None),    
            wa_m  = getattr(r, "freight_wa_m", None),     
            wa_r  = getattr(r, "freight_wa_r", None),     
            nz    = getattr(r, "freight_nz", None),    
        )
        ret.append((r.sku_code, fi))
    return ret



"""
读历史结果表（SkuFreightFee），用于差异对比。 返回 {sku_code: ORM_row}
"""
def query_existing_results_map(db: Session, skus: List[str]) -> Dict[str, Any]:
    if not skus:
        return {}
    rows = (
        db.query(SkuFreightFee)
        .filter(SkuFreightFee.sku_code.in_(skus))
        .all()
    )
    return {r.sku_code: r for r in rows}



"""
批量 upsert 计算结果到 SkuFreightFee
   - 冲突时会把所有列直接覆盖为新值；没有比较旧值与新值的逻辑，不是有变化才更新，只要传入就更新
   - 返回 (inserted_count, updated_count)
"""
# todo 改成有变化才更新
def upsert_freight_results(db: Session, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not rows:
        return (0, 0)

    # 用 PostgreSQL insert ... on conflict
    stmt = insert(SkuFreightFee).values(rows)

    # 冲突时更新的列, # 确保新字段也包含在 rows[0] 中，即由 service 层写入
    update_cols = {  
        k: getattr(stmt.excluded, k)
        for k in rows[0].keys()
        if k != "sku_code"
    }
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[SkuFreightFee.sku_code],
        set_=update_cols,
    )
    res = db.execute(upsert_stmt)
    # 这里无法直接区分 insert / update 数量（取决于返回配置/触发器），
    # 简化处理：全部计为变更；如需精准区分可先查存在与否或用 RETURNING + 标记位
    return (len(rows), 0)





# =========================== price reset use ==========================
'''
读取旧值（对比用）
读取 kogan_sku_freight_fee 中 4 个目标列（+可选的 run/dirty 标记）做旧值对比。
'''
def load_fee_rows_by_skus(db: Session, skus: List[str]) -> Dict[str, dict]:
    if not skus:
        return {}
    sql = text("""
        SELECT sku_code, selling_price, kogan_au_price, kogan_k1_price, kogan_nz_price, kogan_dirty
        FROM kogan_sku_freight_fee
        WHERE sku_code = ANY(:skus)
    """)
    rows = db.execute(sql, {"skus": skus}).mappings().all()
    return {r["sku_code"]: dict(r) for r in rows}


"""
仅更新发生变化的列
    changed: [ (sku_code, { column->new_value, ... }), ... ]
    仅把 dict 里出现的列做 SET；同时统一 SET kogan_dirty=true, last_changed_at=now()
"""
def update_changed_prices(
    db: Session,
    changed: list[tuple[str, dict]],
    *,
    source: str,
    run_id: int | None,
) -> None:
    
    if not changed:
        return
    
    # cols: [old, new]
    for sku_code, cols in changed:
        sets, params = [], {"sku": sku_code, "src": source, "rid": run_id}
        for k, v in cols.items():
            sets.append(f"{k} = :{k}")
            params[k] = v

        # 统一增加脏标记与时间
        sets.extend([
            "kogan_dirty = TRUE",
            "last_changed_at = NOW()",
            "last_changed_source = :src",
            "last_changed_run_id = :rid",
        ])

        sql = text(f"UPDATE kogan_sku_freight_fee SET {', '.join(sets)} WHERE sku_code = :sku")
        db.execute(sql, params)





# ---------- Run 记录 ----------
def create_freight_run(db: Session, product_run_id: Optional[str], 
                       triggered_by: str, candidate_count: int) -> str:
    run = FreightRun(
        id=__import__("uuid").uuid4().hex,   # 与模型的 String(32) 对齐
        status="running",
        triggered_by=triggered_by,           
        product_run_id=product_run_id,       
        candidate_count=candidate_count,     
        changed_count=0,                     # 初始化
    )
    db.add(run)
    db.commit()
    return run.id



"""
将一次运费计算运行( FreightRun )标记为结束。
    - status: "completed" 或 "failed"
    - changed_count: 本次 run 发生变化(需要 upsert) 的 SKU 数
    - message: 可选的备注或错误信息
    返回 True 表示更新成功 / False 表示未找到该 run
"""
def finish_freight_run(db, run_id: str, status: str,
    changed_count: int, message: Optional[str] = None,
) -> bool:
    
    run = db.get(FreightRun, run_id)
    if not run:
        return False

    run.status = status
    run.changed_count = changed_count
    run.finished_at = datetime.utcnow()

    if message:
        # 防御性截断，避免极端长字符串
        run.message = (message[:2000] + "…") if len(message) > 2000 else message

    db.commit()
    return True




"""
基于哈希筛选需要重算
    - 仅返回“需要重算”的 SKU：
    - 条件： sku_info.attrs_hash_current IS DISTINCT FROM kogan_sku_freight_fee.attrs_hash_last_calc
    - 若结果表中不存在该 SKU，也视为需要重算。
    - 幂等 / 去重：相同的候选可能被多次触发（人工/系统重试、Webhook 重放等）。哈希过滤能把已经算过、
    - 结果没变的 SKU 直接跳过
"""
def filter_need_recalc(db: Session, skus: List[str]) -> List[str]:
    if not skus:
        return []
    
    # sku的hash
    cur = (
        db.query(SkuInfo.sku_code, SkuInfo.attrs_hash_current)
        .filter(SkuInfo.sku_code.in_(skus))
        .all()
    )
    cur_map = {r.sku_code: r.attrs_hash_current for r in cur}

    # freight的hash
    old = (
        db.query(SkuFreightFee.sku_code, SkuFreightFee.attrs_hash_last_calc)
        .filter(SkuFreightFee.sku_code.in_(skus))
        .all()
    )
    old_map = {r.sku_code: r.attrs_hash_last_calc for r in old}

    out: List[str] = []
    for sku, h in cur_map.items():
        if old_map.get(sku) != h:
            out.append(sku)
    return out




"""
提供给template流程
读取运费结果，返回 {sku: {shipping_ave, cubic_weight, ...}}。
"""
def load_freight_map(db: Session, skus: List[str]) -> Dict[str, Dict[str, object]]:

    if not skus:
        return {}
    rows: List[SkuFreightFee] = (
        db.query(SkuFreightFee)
        .filter(SkuFreightFee.sku_code.in_(skus))
        .all()
    )
    out: Dict[str, Dict[str, object]] = {}
    for r in rows:
        out[r.sku_code] = {
            "shipping_ave": getattr(r, "shipping_ave", None),
            "cubic_weight": getattr(r, "cubic_weight", None),
        }
    return out
