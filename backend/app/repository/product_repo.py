
# product database repository

from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, Dict, Any, List, Iterator, Tuple, Set
from decimal import Decimal
import io, csv, json
import math
import uuid

import sqlalchemy as sa
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import logging

from sqlalchemy.sql.elements import BindParameter, ClauseElement
from sqlalchemy.orm.attributes import QueryableAttribute
from decimal import Decimal

from app.db.model.product import SkuInfo, ProductSyncCandidate, ProductSyncChunk


'''
 定义“参与对比/写库的字段白名单”。
 作用：做 diff 时只比较这些字段，避免无关字段导致“假变更”。
 Upsert 时只更新这些字段，减少写放大。以 sku_info 规范字段为准
'''
SYNC_FIELDS = [
    "sku_code", "brand", "stock_qty", 
    "supplier", "ean_code",
    "price", "rrp_price", "special_price", "special_price_end_date", "shopify_price",
    "shopify_variant_id",
    "weight", "length", "width", "height", "cbm",
    "product_tags",

    "freight_act", "freight_nsw_m", "freight_nsw_r", "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r", "remote", "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r", "freight_vic_m", "freight_vic_r", "freight_wa_m",
    "freight_wa_r", "freight_nz",
    "attrs_hash_current",           # 新增：后续增量/5.3 计算需要
]


def _is_sqlalchemy_expression(value: Any) -> bool:
    """
    Detect whether a value is any SQLAlchemy SQL expression, column attribute, or bind.
    """
    if isinstance(value, (ClauseElement, BindParameter, QueryableAttribute)):
        return True
    if hasattr(value, "__clause_element__"):
        return True
    return False


# 前端表格用到的主要字段（与 /products 返回一致
_PRODUCT_EXPORT_COLUMNS = [
    "sku_code", "brand", "stock_qty", "product_tags",
    "price", "rrp_price", "special_price", "special_price_end_date", "shopify_price",
    "weight", "length", "width", "height", "cbm", 
    
    "freight_act", "freight_nsw_m", "freight_nsw_r", "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r", "remote", "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r", "freight_vic_m", "freight_vic_r", "freight_wa_m",
    "freight_wa_r", "freight_nz",
    "updated_at", 
]
_PRODUCT_CSV_HEADERS = _PRODUCT_EXPORT_COLUMNS[:]  # 头 = 同名



# ========= 基础查询：商品 tags & 列表 =========
def fetch_distinct_product_tags(db: Session) -> List[str]:
    """
    查询 sku_info 中所有唯一的 product_tags。
    结果按照字母顺序返回，忽略空值。
    """
    sql = text(
        """
        SELECT DISTINCT tag
          FROM (
                SELECT NULLIF(trim(elem.tag_value), '') AS tag
                  FROM sku_info
                 CROSS JOIN LATERAL jsonb_array_elements_text(product_tags) AS elem(tag_value)
               ) AS tags
         WHERE tag IS NOT NULL
         ORDER BY tag
        """
    )
    return db.execute(sql).scalars().all()


def fetch_products_page(
    db: Session,
    *,
    sku_prefix: Optional[str],
    tags: Optional[List[str]],
    page: int,
    page_size: int,
) -> tuple[List[Dict[str, Any]], int]:
    """
    根据筛选条件分页查询 sku_info。
    返回 (rows, total)；rows 是 dict 列表，字段与 Product 响应模型对齐。
    """
    conditions = ["1=1"]
    params: Dict[str, Any] = {}

    if sku_prefix:
        conditions.append("sku_code ILIKE :sku_prefix")
        params["sku_prefix"] = f"{sku_prefix}%"

    tag_values: List[str] = []
    if tags:
        tag_values = [t.strip().lower() for t in tags if t and t.strip()]
        if tag_values:
            placeholders = []
            for idx, value in enumerate(tag_values):
                key = f"tag_{idx}"
                placeholders.append(f":{key}")
                params[key] = value
            conditions.append(
                f"""
                EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(product_tags) AS elem(tag_value)
                     WHERE lower(elem.tag_value) IN ({', '.join(placeholders)})
                )
                """
            )

    where_sql = " AND ".join(conditions)
    base_sql = f"FROM sku_info WHERE {where_sql}"

    total_sql = f"SELECT COUNT(*) {base_sql}"
    total = db.execute(text(total_sql), params).scalar_one()

    offset = (page - 1) * page_size
    data_sql = text(
        f"""
        SELECT
            id,
            sku_code,
            shopify_variant_id,
            stock_qty,
            price,
            rrp_price,
            special_price,
            special_price_end_date,
            shopify_price,
            brand,
            weight,
            length,
            width,
            height,
            cbm,
            product_tags,
            attrs_hash_current,
            updated_at,
            freight_act,
            freight_nsw_m,
            freight_nsw_r,
            freight_nt_m,
            freight_nt_r,
            freight_qld_m,
            freight_qld_r,
            remote,
            freight_sa_m,
            freight_sa_r,
            freight_tas_m,
            freight_tas_r,
            freight_vic_m,
            freight_vic_r,
            freight_wa_m,
            freight_wa_r,
            freight_nz
          {base_sql}
         ORDER BY updated_at DESC NULLS LAST, sku_code ASC
         LIMIT :limit OFFSET :offset
        """
    )

    data_params = params.copy()
    data_params.update({"limit": page_size, "offset": offset})
    rows = db.execute(data_sql, data_params).mappings().all()
    return [dict(r) for r in rows], total


# ========= 读取 sku 的现有快照 =========
# 场景：给 orchestration/product_sync 用，读取旧记录做对比
def load_existing_by_skus(db, skus: list[str]) -> dict[str, dict]:
    if not skus:
        return {}
    
    # 只取 SYNC_FIELDS 列能减少 I/O
    cols = [getattr(SkuInfo, c) for c in SYNC_FIELDS]
    out: dict[str, dict] = {}
    
    # 大 IN 列表可以分批执行（例如按 1000 一批），避免参数列表过大、SQL 文本过长
    BATCH = 1000
    for i in range(0, len(skus), BATCH):
        batch = skus[i:i+BATCH]
        rows = db.execute(select(*cols).where(SkuInfo.sku_code.in_(batch))).all()

        # rows 是元组，构造成 dict
        for rec in rows:
            row_dict = dict(zip(SYNC_FIELDS, rec))
            out[row_dict["sku_code"]] = row_dict

    return out


# ========= 批量读取 variant_id 映射（按 sku_code） =========
def load_variant_ids_by_skus(db: Session, skus: list[str]) -> dict[str, Optional[str]]:  
    if not skus:
        return {}
    out: dict[str, Optional[str]] = {}
    BATCH = 1000
    for i in range(0, len(skus), BATCH):
        batch = skus[i:i+BATCH]
        rows = db.execute(
            select(SkuInfo.sku_code, SkuInfo.shopify_variant_id)
            .where(SkuInfo.sku_code.in_(batch))
        ).all()
        for sku, vid in rows:
            out[sku] = vid
    return out



# ===================== 流式拉取“price reset 候选” =====================
"""
  流式拉取“price reset”候选 SKU：special_price_end_date 早于或等于 target_date。
  仅返回 sku_code，按 sku 排序分页。
"""
def iter_price_reset_candidates(
    db: Session,
    *,
    target_date,
    page_size: int = 1000,
) -> Iterator[Tuple[str]]:

    offset = 0
    sql = text("""
        SELECT sku_code
        FROM sku_info
        WHERE special_price_end_date IS NOT NULL
          AND special_price_end_date <= :target_date
          AND price IS NOT NULL
        ORDER BY sku_code
        LIMIT :limit OFFSET :offset
    """)

    while True:
        rows = db.execute(sql, {"target_date": target_date, "limit": page_size, "offset": offset}).mappings().all()
        if not rows:
            break
        for r in rows:
            yield r["sku_code"]
        offset += page_size



# ========= 对比新旧快照，找出变更字段 =========
# 场景：给 orchestration/product_sync 用，找出有变化的 SKU 及其变化字段
# 一次对比一个sku
def diff_snapshot(old: dict|None, new: dict) -> dict:

    # 先比对 attrs_hash_current, 若新旧哈希一致，则直接跳过逐字段对比
    base = old or {}
    if base.get("attrs_hash_current") == new.get("attrs_hash_current"):
        special = set()
        for field in ("shopify_variant_id", "shopify_price", "product_tags"):
            if base.get(field) != new.get(field):
                special.add(field)
        if special:
            return special
        return set()  # 完全一致，省去逐字段比较

    changed = set()
    for k in SYNC_FIELDS:
        if base.get(k) != new.get(k):
            changed.add(k)
    return changed



"""
批量 upsert SKU 信息
   - 把一批标准化后的 SKU 行批量 Upsert 到 sku_info 表
   - Postgres 的 INSERT ... ON CONFLICT (sku_code) DO UPDATE
   - only_update_when_changed=True：只在确有变化时才 UPDATE（避免刷新 updated_at）
   - 批量 UPSERT sku_info；只有当 SYNC_FIELDS 任一字段真的发生变化时，才刷新 last_changed_at = now()。
   - 返回尝试写入的行数（不是受影响行数）。
   - 场景：给 orchestration/product_sync 用，批量写入变更的 SKU 记录
"""
def bulk_upsert_sku_info(db, rows: list[dict], *, only_update_when_changed: bool=False) -> None:
    """
    把每条记录逐条 upsert。only_update_when_changed=True 时会先比对旧快照，未变更则跳过。
    """
    if not rows:
        return

    total = len(rows)

    try:
        deduped: dict[str, dict] = {}
        for row in rows:
            sku = row.get("sku_code")
            if not sku:
                continue
            clean: dict[str, Any] = {}
            for key, value in row.items():
                if isinstance(value, BindParameter):
                    value = getattr(value, "effective_value", getattr(value, "value", None))
                if _is_sqlalchemy_expression(value):
                    raise ValueError(
                        f"bulk_upsert_sku_info sku={sku!r} field={key!r} is SQL expression ({type(value)!r}); provide plain Python value"
                    )
                if isinstance(value, Decimal) and not value.is_finite():
                    value = None
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    value = None
                clean[str(key)] = value
            deduped[sku] = clean

        payload_rows = list(deduped.values())
        if not payload_rows:
            return

        existing_map: dict[str, dict] = {}
        if only_update_when_changed:
            skus = [r["sku_code"] for r in payload_rows if r.get("sku_code")]
            if skus:
                columns = [getattr(SkuInfo, c) for c in SYNC_FIELDS]
                existing_rows = (
                    db.execute(
                        select(*columns).where(SkuInfo.sku_code.in_(skus))
                    )
                    .all()
                )
                for rec in existing_rows:
                    row_dict = dict(zip(SYNC_FIELDS, rec))
                    existing_map[row_dict["sku_code"]] = row_dict

        now_expr = func.now()

        for row in payload_rows:
            sku = row.get("sku_code")
            if not sku:
                continue

            if only_update_when_changed:
                current = existing_map.get(sku)
                if current is not None:
                    if all(row.get(field) == current.get(field) for field in SYNC_FIELDS):
                        continue

            stmt = insert(SkuInfo).values(row)
            updates = {field: getattr(stmt.excluded, field) for field in SYNC_FIELDS}
            updates.update(
                {
                    "updated_at": now_expr,
                    "last_changed_at": now_expr,
                }
            )
            db.execute(
                stmt.on_conflict_do_update(
                    index_elements=[SkuInfo.sku_code],
                    set_=updates,
                )
            )
    except Exception:
        logger.exception(
            "bulk_upsert_sku_info failed: rows=%d only_update=%s sample=%s",
            total,
            only_update_when_changed,
            rows[:3],
        )
        raise


def bulk_upsert_sku_info_2(
    db: Session,
    changed: list[tuple[str, dict]],
) -> int:
    """
    列级 upsert：仅更新传入字段，未提供的列保持原值（类似 freight_repo.update_changed_prices）。
    适用于“已知哪些列发生变化”的场景。
    """
    if not changed:
        return 0

    # 去重：同一 SKU 只保留最后一次出现，并合并字段
    merged: dict[str, dict[str, Any]] = {}
    for sku, fields in changed:
        if not sku or not fields:
            continue
        payload = merged.setdefault(str(sku), {})
        for key, value in fields.items():
            payload[str(key)] = value

    if not merged:
        return 0

    model_cols: set[str] = set(SkuInfo.__table__.columns.keys())
    meta_cols = {
        "id",
        "sku_code",
        "created_at",
        "updated_at",
        "last_changed_at",
    }

    data_cols: set[str] = set()
    for fields in merged.values():
        for col in fields.keys():
            if col in model_cols and col not in meta_cols:
                data_cols.add(col)

    rows_to_insert: list[dict[str, Any]] = []
    now_expr = sa.func.now()
    for sku, fields in merged.items():
        row: dict[str, Any] = {"sku_code": sku}
        for col in data_cols:
            row[col] = fields.get(col)
        row["updated_at"] = now_expr
        row["last_changed_at"] = now_expr
        rows_to_insert.append(row)

    if not rows_to_insert:
        return 0

    table_cols = SkuInfo.__table__.c
    total = 0
    BATCH = 1000
    for idx in range(0, len(rows_to_insert), BATCH):
        chunk = rows_to_insert[idx : idx + BATCH]
        stmt = insert(SkuInfo).values(chunk)
        excluded = stmt.excluded

        set_updates: dict[str, Any] = {}
        for col in sorted(data_cols):
            set_updates[col] = sa.func.coalesce(getattr(excluded, col), getattr(table_cols, col))

        changed_terms = [
            sa.and_(
                getattr(excluded, col).isnot(None),
                getattr(excluded, col).is_distinct_from(getattr(table_cols, col)),
            )
            for col in data_cols
        ]
        if changed_terms:
            changed_pred = sa.or_(*changed_terms)
        else:
            changed_pred = sa.literal(False)

        set_updates.update(
            {
                "updated_at": now_expr,
                "last_changed_at": sa.case(
                    (changed_pred, now_expr),
                    else_=SkuInfo.last_changed_at,
                ),
            }
        )

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=[SkuInfo.sku_code],
            set_=set_updates,
        )
        res = db.execute(upsert_stmt)
        total += int(res.rowcount or 0)

    return total


"""
批量保存变更候选记录
场景：给 orchestration/product_sync 用，保存“本次 run 中字段有变化的 SKU”及其变更字段/新值
    - 把“确实变了”的 SKU 的变动字段，以 (run_id, sku) 为幂等键批量 upsert 到候选表
    - 冲突覆盖保证“同 run、同 SKU”只保留最后一次写入
    - new_s: 变化字段子集
    - 把“有变化的 SKU”写入 product_sync_candidates（候选表），给后续 5.3 只处理增量
"""
def save_candidates(db: Session, run_id: str, tuples: list[tuple[str, dict]]) -> int:  
    """
    将 (sku, new_partial_fields) 批量 upsert 到 product_sync_candidates
    覆盖式策略：同一 (run_id, sku) 后写覆盖先写
    表结构：
      change_mask: JSONB 数组（如 ["price","weight"]）
      new_snapshot: JSONB 对象（如 {"price": 19.99, "weight": 2.4}）
    """
    if not tuples:
        return 0

    try:
        rows = []
        seen = set()    # 同一次调用内的去重

        for sku, new_s in tuples:
            if not new_s:
                continue
            key = (run_id, sku)
            if key in seen:
                continue
            seen.add(key)

            change_mask = {str(k): True for k in new_s.keys()}
            change_count = len(change_mask)
            if change_count == 0:
                continue

            snapshot = _to_jsonable(new_s)
            
            rows.append({
                "run_id": run_id,
                "sku_code": sku,
                "change_mask": change_mask,  # jsonb object
                "new_snapshot": snapshot,       # 就是 new_s 本身，保存变化字段的新值，便于下游有选择地处理（比如只看价格变化）
                "change_count": change_count,
            })

        if not rows:
            return 0
        
        # debug: print/log the size of rows being upserted
        logger.info("[save_candidates] candidate rows size: %d", len(rows))
        print(f"[save_candidates] candidate rows size: {len(rows)}")

        stmt = insert(ProductSyncCandidate).values(rows)   # 一次写入

        # 覆盖式 upsert：同键直接用新值覆盖（简单稳妥）
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "sku_code"],   # 依赖唯一索引/约束 UniqueConstraint
            set_={
                "change_mask": stmt.excluded.change_mask,
                "new_snapshot": stmt.excluded.new_snapshot,
                "change_count": stmt.excluded.change_count,
                "updated_at": func.now(),
            },
        )

        res = db.execute(upsert_stmt)
        return res.rowcount or 0
    except Exception:
        logger.exception(
            "save_candidates failed: run=%s tuples=%d sample=%s",
            run_id,
            len(tuples),
            tuples[:3],
        )
        raise





def _to_jsonable(value):
    """
    递归把任意 Python 值转换为 JSON 可序列化的原生类型：
    - Decimal -> float（NaN/Inf -> None）
    - date/datetime -> ISO 字符串
    - uuid.UUID -> 字符串
    - tuple/set/list -> list
    - dict -> dict（键统一转 str）
    其它（str/int/float/bool/None）原样返回；非法 float（NaN/Inf）转 None
    """
    if isinstance(value, Decimal):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, uuid.UUID):
        return str(value)

    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    return value





# ===================== 加载计算需要的州运费/重量等快照 =====================
"""
为 compute_all 准备输入的“各州运费 + 重量/体积等”字段快照。
仅选必要列，减少 IO。
 """
def load_state_freight_by_skus(db: Session, skus: List[str]) -> Dict[str, dict]:
    
    if not skus:
        return {}
    
    sql = text(f"""
        SELECT
            sku_code,
            price,
            special_price,
            special_price_end_date,
            weight, length, width, height, cbm,
            freight_act, freight_nsw_m, freight_nsw_r, freight_qld_m, freight_qld_r,
            freight_sa_m, freight_sa_r, freight_tas_m, freight_tas_r,
            freight_vic_m, freight_vic_r, freight_wa_m, freight_wa_r,
            remote, freight_nz
        FROM sku_info
        WHERE sku_code = ANY(:skus)
    """)
    rows = db.execute(sql, {"skus": skus}).mappings().all()
    return {r["sku_code"]: dict(r) for r in rows}



# ===================== SKU 清理相关 =====================
def collect_shopify_skus_for_run(db: Session, run_id: str) -> Set[str]:
    """
    从 manifest（product_sync_chunks.sku_codes）中提取本次 run 覆盖的 Shopify SKU。
    兼容历史格式（对象、数组、字符串），返回去重集合。
    """
    sql = text(
        """
        SELECT DISTINCT
               CASE
                   WHEN jsonb_typeof(elem) = 'object' AND elem ? 'sku'
                        THEN NULLIF(trim(elem->>'sku'), '')
                   WHEN jsonb_typeof(elem) = 'array' AND jsonb_array_length(elem) > 0
                        THEN NULLIF(trim(elem->>0), '')
                   WHEN jsonb_typeof(elem) = 'string'
                        THEN NULLIF(trim(both '"' FROM elem::text), '')
                   ELSE NULL
               END AS sku
          FROM product_sync_chunks AS chunk
         CROSS JOIN LATERAL jsonb_array_elements(chunk.sku_codes) AS elem
         WHERE chunk.run_id = :run_id
        """
    )
    rows = db.execute(sql, {"run_id": run_id}).mappings().all()
    return {row["sku"] for row in rows if row["sku"]}



def purge_sku_info_absent_from(db: Session, keep_skus: Iterable[str]) -> List[str]:
    """
    删除 sku_info 中不在 keep_skus 集合内的记录，返回被删除的 SKU 列表。
    传入空集合时直接跳过，避免误删。
    """
    keep_list = [str(s).strip() for s in keep_skus if s and str(s).strip()]
    if not keep_list:
        return []

    sql = text(
        """
        DELETE FROM sku_info AS s
         WHERE NOT EXISTS (
               SELECT 1
                 FROM unnest(:keep_skus::text[]) AS keep(sku_code)
                WHERE keep.sku_code = s.sku_code
         )
        RETURNING s.sku_code
        """
    )
    deleted = db.execute(sql, {"keep_skus": keep_list}).scalars().all()
    return list(deleted)


# ---------------- Manifest 封装（pending / running / succeeded / failed）---------------- #
def upsert_chunk_pending(db: Session, run_id: str, chunk_idx: int, sku_codes: list[Any]) -> None:  # [NEW]
    """切片时写入/刷新 manifest 为 pending"""
    if not sku_codes:
        sku_codes = []
    sku_count = len(sku_codes)
    insert_stmt = insert(ProductSyncChunk).values({
        "run_id": run_id,
        "chunk_idx": chunk_idx,
        "status": "pending",
        "sku_codes": sku_codes,
        "sku_count": sku_count,
        "dsz_missing": 0, "dsz_failed_batches": 0, "dsz_failed_skus": 0,
        "dsz_requested_total": 0, "dsz_returned_total": 0,
    })
    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["run_id", "chunk_idx"],
        set_={
            "status": "pending",
            "sku_codes": insert_stmt.excluded.sku_codes,
            "sku_count": insert_stmt.excluded.sku_count,
            "updated_at": func.now(),
        }
    )
    db.execute(stmt)


def mark_chunk_running(db: Session, run_id: str, idx: int) -> None:  # [NEW]
    db.execute(
        sa.update(ProductSyncChunk)
        .where(ProductSyncChunk.run_id==run_id, ProductSyncChunk.chunk_idx==idx)
        .values(status="running", started_at=func.now())
    )


"""
    将分片标记为 succeeded，并写入 DSZ 统计（计数 + 明细）。
    若模型不存在明细列，自动忽略（向后兼容）。
"""
def mark_chunk_succeeded(db: Session, run_id: str, idx: int, stats: dict | None) -> None:
    stats = stats or {}

    updates = {
        "status": "succeeded",
        "finished_at": func.now(),
        "dsz_missing": int(stats.get("missing_count") or 0),
        "dsz_failed_batches": int(stats.get("failed_batches_count") or 0),
        "dsz_failed_skus": int(stats.get("failed_skus_count") or 0),
        "dsz_requested_total": int(stats.get("requested_total") or 0),
        "dsz_returned_total": int(stats.get("returned_total") or 0),
    }

    if hasattr(ProductSyncChunk, "dsz_missing_sku_list"):
        updates["dsz_missing_sku_list"] = stats.get("missing_sku_list") or []
    if hasattr(ProductSyncChunk, "dsz_failed_sku_list"):
        updates["dsz_failed_sku_list"] = stats.get("failed_sku_list") or []
    if hasattr(ProductSyncChunk, "dsz_extra_sku_list"):
        updates["dsz_extra_sku_list"] = stats.get("extra_sku_list") or []

    result = db.execute(
        sa.update(ProductSyncChunk)
        .where(
            ProductSyncChunk.run_id == run_id,
            ProductSyncChunk.chunk_idx == idx,
        )
        .values(**updates)
    )
    if result.rowcount == 0:
        logger.warning(
            "mark_chunk_succeeded updated 0 rows: run=%s idx=%s",
            run_id,
            idx,
        )


def mark_chunk_failed(db: Session, run_id: str, idx: int, err: Exception | str) -> None:  # [NEW]
    result = db.execute(
        sa.update(ProductSyncChunk)
        .where(
            ProductSyncChunk.run_id == run_id,
            ProductSyncChunk.chunk_idx == idx,
        )
        .values(
            status="failed",
            finished_at=func.now(),
            last_error=str(err)[:2000],
        )
    )
    if result.rowcount == 0:
        logger.warning(
            "mark_chunk_failed updated 0 rows: run=%s idx=%s err=%s",
            run_id,
            idx,
            err,
        )






# ========= 列出今天有变更的 SKU =========
# 场景：给运费计算服务调用，列出今天有变更的 SKU 列表
def list_today_changed_skus(db, tz_name="Australia/Melbourne"):
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    start_local = datetime.combine(today, time.min, tzinfo=tz)
    end_local   = start_local + timedelta(days=1)
    start_utc, end_utc = start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

    q = (sa.select(SkuInfo.sku_code)
         .where(SkuInfo.last_changed_at >= start_utc)
         .where(SkuInfo.last_changed_at <  end_utc)
         .order_by(SkuInfo.sku_code))
    return db.execute(q).scalars().all()



# ======== 导出商品列表为 CSV（流式） =========
def export_products_csv_iter(
    *, db=None, use_mock: bool, mock_rows: Optional[Iterable[Dict[str, Any]]] = None,
    sku_prefix: Optional[str] = None, tags_csv: Optional[str] = None,
    prefer_sql: bool = True, flush_bytes: int = 64 * 1024,
):
    """统一入口：现在用 mock；以后切 DB 仅改 use_mock=False。"""
    if use_mock:
        if mock_rows is None:
            raise ValueError("use_mock=True 需提供 mock_rows")
        return export_products_csv_iter_mock(
            mock_rows, sku_prefix=sku_prefix, tags_csv=tags_csv, flush_bytes=flush_bytes
        )
    
    # DB
    return export_products_csv_iter_sql(
        db, sku_prefix=sku_prefix, tags_csv=tags_csv, flush_bytes=flush_bytes
    )


'''
 从内存 mock 过滤并导出为 CSV
'''
def export_products_csv_iter_mock(
    rows: Iterable[Dict[str, Any]],
    *, sku_prefix: Optional[str], tags_csv: Optional[str],
    flush_bytes: int = 64 * 1024,
):
    """从内存 mock 过滤并导出为 CSV（与 /products 筛选完全一致【:contentReference[oaicite:3]{index=3}】）"""
    data = _filter_products_in_memory(rows, sku_prefix, tags_csv)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(_PRODUCT_CSV_HEADERS)
    yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    for r in data:
        row = {k: r.get(k) for k in _PRODUCT_CSV_HEADERS}
        # product_tags 序列化成人类可读 JSON
        if isinstance(row.get("product_tags"), (list, dict)):
            row["product_tags"] = json.dumps(row["product_tags"], ensure_ascii=False)
        w.writerow([row.get(k) for k in _PRODUCT_CSV_HEADERS])

        if buf.tell() >= flush_bytes:
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    left = buf.getvalue()
    if left:
        yield left


# ========= 内存中过滤商品列表（给 /products 导出mock 用） =========
def _filter_products_in_memory(
    rows: Iterable[Dict[str, Any]],
    sku_prefix: Optional[str],
    tags_csv: Optional[str],
) -> List[Dict[str, Any]]:
    data = list(rows)
    if sku_prefix:
        p = sku_prefix.lower()
        data = [r for r in data if (r.get("sku_code") or "").lower().startswith(p)]
    if tags_csv:
        wanted = {t.strip().lower() for t in tags_csv.split(",") if t.strip()}
        if wanted:
            data = [
                r for r in data
                if any((tg or "").lower() in wanted for tg in (r.get("tags") or []))
            ]
    return data




'''
  DB 版（PostgreSQL 原生 SQL 流式；与 SkuInfo 表字段对齐
'''
def export_products_csv_iter_sql(
    db,
    *, sku_prefix: Optional[str], tags_csv: Optional[str],
    flush_bytes: int = 64 * 1024,
):
    conds, params = ["1=1"], {}
    if sku_prefix:
        conds.append("sku_code ILIKE :sku")
        params["sku"] = sku_prefix + "%"

    if tags_csv:
        tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
        if tags:
            ph, i = [], 0
            for v in tags:
                k = f"t{i}"; i += 1
                params[k] = v
                ph.append(f":{k}")
            # SkuInfo.product_tags JSONB：任意命中
            conds.append(f"product_tags ?| ARRAY[{','.join(ph)}]")

    where_sql = " AND ".join(conds)

    columns_sql = ",\n             ".join(_PRODUCT_EXPORT_COLUMNS)
    sql = f"""
      SELECT {columns_sql}
        FROM sku_info
       WHERE {where_sql}
    ORDER BY sku_code
    """

    conn = db.connection()
    rs = conn.execution_options(stream_results=True).execute(text(sql), params)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(_PRODUCT_CSV_HEADERS)
    yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    keys = rs.keys()
    for rec in rs:
        d = dict(zip(keys, rec))
        if isinstance(d.get("product_tags"), (list, dict)):
            d["product_tags"] = json.dumps(d["product_tags"], ensure_ascii=False)
        # special_price_end_date/updated_at 让数据库按默认文本输出（或自行格式化）
        w.writerow([d.get(k) for k in _PRODUCT_CSV_HEADERS])

        if buf.tell() >= flush_bytes:
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    left = buf.getvalue()
    if left:
        yield left




"""
 查询sku_info表，获取产品相关信息（提供给kogan template流程）
     - 读取产品信息，返回 {sku: 字段字典}，字段名尽量与 service 映射需要的 keys 对齐。
     - 这里采用“宽表式”安全取值：即使某些列不存在也不报错（getattr 兜底 None）。
     - 获取产品信息: sku, rrp, ean_code, stock_qty, brand, sku2? 
"""
def load_products_map(db: Session, skus: List[str]) -> Dict[str, Dict[str, object]]:
    if not skus:
        return {}

    rows: List[SkuInfo] = (
        db.query(SkuInfo)
        .filter(SkuInfo.sku_code.in_(skus))
        .all()
    )

    out: Dict[str, Dict[str, object]] = {}
    for r in rows:
        out[r.sku_code] = {
            "rrp": getattr(r, "rrp_price", None),
            "barcode": getattr(r, "ean_code", None),
            "stock": getattr(r, "stock_qty", None),
            "brand": getattr(r, "brand", None),
            "weight": getattr(r, "weight", None),

            # "kogan_first_price": getattr(r, "kogan_first_price", None),
            # "handling_days": getattr(r, "handling_days", None),
            # "title": getattr(r, "title", None),
            # "description": getattr(r, "description", None),
            # "subtitle": getattr(r, "subtitle", None),
            # "whats_in_the_box": getattr(r, "whats_in_the_box", None),
            # "category": getattr(r, "category", None),
        }
    return out
logger = logging.getLogger(__name__)
