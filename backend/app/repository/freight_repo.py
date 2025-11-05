
# 运费计算结果相关的 DB 操作

from __future__ import annotations
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Optional, Tuple, Any
import json

import sqlalchemy as sa
from sqlalchemy import select, text, Numeric, Boolean, DateTime as SA_DateTime, Integer, Float
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.model.product import SkuInfo, ProductSyncCandidate   # 商品信息表
from app.db.model.freight import SkuFreightFee, FreightRun
from app.services.freight.freight_compute import FreightInputs
# from app.db.model.shopify_jobs import ShopifyUpdateJob           # 待派发到同步shopify的作业表

from app.utils.attrs_hash import FREIGHT_HASH_FIELDS             # 作为“运费相关字段集合
FREIGHT_RELEVANT_FIELDS: set[str] = set(FREIGHT_HASH_FIELDS)


"""针对前端运费列表的查询方法"""
def fetch_shipping_types(db: Session) -> List[str]:
    """
    从 kogan_sku_freight_fee 表中查询去重后的 shipping_type。
    仅返回非空值，并按字母顺序排序。
    """
    sql = text(
        """
        SELECT DISTINCT shipping_type
          FROM kogan_sku_freight_fee
         WHERE shipping_type IS NOT NULL
           AND trim(shipping_type) <> ''
         ORDER BY shipping_type
        """
    )
    return db.execute(sql).scalars().all()


def fetch_freight_results_page(
    db: Session,
    *,
    sku_prefix: Optional[str],
    tags: Optional[List[str]],
    shipping_types: Optional[List[str]],
    page: int,
    page_size: int,
) -> tuple[List[Dict[str, Any]], int]:
    """
    根据筛选条件分页查询运费结果。
    返回 (rows, total)，其中 rows 是字段名与前端展示保持一致的字典列表。
    """

    conditions: List[str] = ["1=1"]
    params: Dict[str, Any] = {}

    if sku_prefix:
        conditions.append("f.sku_code ILIKE :sku_prefix")
        params["sku_prefix"] = f"{sku_prefix}%"

    if shipping_types:
        values = [s.strip() for s in shipping_types if s and s.strip()]
        if values:
            placeholders: List[str] = []
            for idx, value in enumerate(values):
                key = f"st_{idx}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(f"f.shipping_type IN ({', '.join(placeholders)})")

    if tags:
        lowered = [t.strip().lower() for t in tags if t and t.strip()]
        if lowered:
            placeholders: List[str] = []
            for idx, value in enumerate(lowered):
                key = f"tag_{idx}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(si.product_tags) AS elem(tag_value)
                     WHERE lower(elem.tag_value) IN ({', '.join(placeholders)})
                )
                """
            )

    where_sql = " AND ".join(conditions)
    base_sql = f"""
        FROM kogan_sku_freight_fee AS f
        LEFT JOIN sku_info AS si ON si.sku_code = f.sku_code
       WHERE {where_sql}
    """

    total_sql = text(f"SELECT COUNT(*) {base_sql}")
    total = db.execute(total_sql, params).scalar_one()

    offset = (page - 1) * page_size
    data_sql = text(
        f"""
        SELECT
            f.sku_code,
            f.shipping_type,
            f.adjust,
            f.same_shipping,
            f.shipping_ave,
            f.shipping_ave_m,
            f.shipping_ave_r,
            f.shipping_med,
            f.shipping_med_dif,
            f.remote_check,
            f.rural_ave,
            f.weighted_ave_s,
            f.cubic_weight,
            f.weight,
            f.price_ratio,
            f.selling_price,
            f.shopify_price,
            f.kogan_au_price,
            f.kogan_k1_price,
            f.kogan_nz_price,
            f.updated_at,
            COALESCE(si.product_tags, '[]'::jsonb) AS product_tags,
            si.price AS cost
          {base_sql}
         ORDER BY f.updated_at DESC NULLS LAST, f.sku_code ASC
         LIMIT :limit OFFSET :offset
        """
    )

    data_params = params.copy()
    data_params.update({"limit": page_size, "offset": offset})
    rows = db.execute(data_sql, data_params).mappings().all()

    return [dict(row) for row in rows], total



"""
轻量输入结构：与 app.services.freight_compute 里的 FreightInputs 字段保持一致。
这里不做运算，仅承载数据（为了避免 repo 依赖 service）。
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
            # length=getattr(r, "length", None),
            # width=getattr(r, "width", None),
            # height=getattr(r, "height", None),
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
读取 kogan_sku_freight_fee 中业务字段（不含时间列），用于新旧结果对比。
'''
def load_fee_rows_by_skus(db: Session, skus: List[str]) -> Dict[str, dict]:
    if not skus:
        return {}
    sql = text("""
        SELECT
            sku_code,
            adjust,
            same_shipping,
            shipping_ave,
            shipping_ave_m,
            shipping_ave_r,
            shipping_med,
            remote_check,
            rural_ave,
            weighted_ave_s,
            shipping_med_dif,
            weight,
            cubic_weight,
            shipping_type,
            price_ratio,
            selling_price,
            shopify_price,
            kogan_au_price,
            kogan_k1_price,
            kogan_nz_price,
            attrs_hash_last_calc
        FROM kogan_sku_freight_fee
        WHERE sku_code = ANY(:skus)
    """)
    rows = db.execute(sql, {"skus": skus}).mappings().all()
    return {r["sku_code"]: dict(r) for r in rows}




"""
    列级批量“upsert”：
      - 对于每个 (sku, fields)，只更新出现于 fields 的列；
      - 表里不存在该 sku 时自动 INSERT；
      - 存在时 DO UPDATE，未传的列用 COALESCE 保持原值不变；
      - 实际有列值变化时才刷新 last_changed_at。
    返回受影响的（INSERT+UPDATE）行数近似值（PostgreSQL 的 rowcount 统计策略下）。

    参数示例：
      changed = [
        ("SKU-001", {"kogan_au_price": Decimal("99.90"), "shipping_type": "Kogan"}),
        ("SKU-002", {"weight": Decimal("2.3")}),
      ]
    """
def update_changed_prices(
    db: Session,
    changed: List[Tuple[str, Dict[str, Any]]],
    *,
    source: str,
    run_id: str | None,
) -> int:
    
    if not changed:
        return 0

    # 1) 收集本批出现的列（仅表中真实存在的列）
    model_cols: set[str] = set(SkuFreightFee.__table__.columns.keys())
    # 不允许外部直接覆盖的元数据列（下面统一在 set_ 中维护）
    meta_cols = {
        "id", "sku_code", "updated_at",
        "last_changed_at", "last_changed_source", "last_changed_run_id",
        # "kogan_dirty_au", "kogan_dirty_nz",
        # 视你的模型情况追加其它不希望被直接覆盖的列
    }

    data_cols: set[str] = set()
    for _, fields in changed:
        if not fields:
            continue
        for k in fields.keys():
            if k in model_cols and k not in meta_cols:
                data_cols.add(k)

    # 若本批没有任何可更新的业务列，也要保证能写入/插入占位并打上元数据
    # ON CONFLICT SET 中将只包含 meta 的更新
    all_rows: List[Dict[str, Any]] = []

    # 2) 构造 INSERT 的行：未出现的列统一给 None
    #    这样在 ON CONFLICT DO UPDATE 时，COALESCE(EXCLUDED.col, table.col) 可保持老值
    for sku, fields in changed:
        row: Dict[str, Any] = {"sku_code": sku}

        # 只把允许更新的业务列放入行中；没出现的字段不设置（等价于 None）
        for c in data_cols:
            row[c] = fields.get(c, None)

        # 统一的元数据（INSERT 时生效；UPDATE 时在 set_ 里控制）
        row["kogan_dirty_au"] = True
        row["kogan_dirty_nz"] = True
        row["last_changed_source"] = source
        row["last_changed_run_id"] = run_id
        row["last_changed_at"] = sa.func.now()
        row["updated_at"] = sa.func.now()

        all_rows.append(row)

    # 3) 生成 upsert 语句
    stmt = insert(SkuFreightFee).values(all_rows)

    # 对业务列使用 COALESCE(EXCLUDED.col, table.col) 实现“只更新传入的列”
    set_updates: Dict[str, Any] = {}
    excluded = stmt.excluded
    table = SkuFreightFee.__table__.c

    for c in sorted(data_cols):
        set_updates[c] = sa.func.coalesce(getattr(excluded, c), getattr(table, c))

    # 计算“是否真的有变化”（只看本批传入的列且值非 NULL）
    # changed_pred = OR( EXCLUDED.c IS NOT NULL AND EXCLUDED.c IS DISTINCT FROM table.c, ... )
    changed_terms = [
        sa.and_(
            getattr(excluded, c).isnot(None),
            getattr(excluded, c).is_distinct_from(getattr(table, c)),
        )
        for c in data_cols
    ]
    if changed_terms:
        changed_pred = sa.or_(*changed_terms)
    else:
        # 没有业务列，也不刷新 last_changed_at（但依旧会更新 source/run_id/dirty）
        changed_pred = sa.literal(False)

    # 统一的元数据：只在“真的变化”时刷新 last_changed_at，其它元数据每次写入/更新
    set_updates.update(
        {
            "kogan_dirty_au": True,
            "kogan_dirty_nz": True,
            "last_changed_source": source,
            "last_changed_run_id": run_id,
            "updated_at": sa.func.now(),
            "last_changed_at": sa.case(
                (changed_pred, sa.func.now()),
                else_=SkuFreightFee.last_changed_at,
            ),
        }
    )

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[SkuFreightFee.sku_code],
        set_=set_updates,
    )

    res = db.execute(upsert_stmt)
    # 注意：PG 的 rowcount 对 upsert 行为的统计可能不是严格“变更行数”，但足够用于观测
    return int(res.rowcount or 0)




def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _column_cast(sa_type) -> str:
    if isinstance(sa_type, Numeric):
        return "::numeric"
    if isinstance(sa_type, Integer):
        return "::integer"
    if isinstance(sa_type, Float):
        return "::double precision"
    if isinstance(sa_type, Boolean):
        return "::boolean"
    if isinstance(sa_type, SA_DateTime):
        return "::timestamptz"
    return ""





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
读取运费结果，返回 {sku: {sku, kogan_au_price, kogan first price, shipping, weight(update后的)}}
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
            # "sku": r.sku_code,
            "kogan_au_price": getattr(r, "kogan_au_price", None),
            "kogan_k1_price": getattr(r, "kogan_k1_price", None),
            "kogan_nz_price": getattr(r, "kogan_nz_price", None),
            "shipping_type": getattr(r, "shipping_type", None),
            "weight": getattr(r, "weight", None),
            # "cubic_weight": getattr(r, "cubic_weight", None),
        }
    return out
