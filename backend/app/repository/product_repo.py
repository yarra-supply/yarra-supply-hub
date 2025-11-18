
# product database repository

from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, Dict, Any, List, Iterator, Tuple, Set
from decimal import Decimal
import io, csv
import math

import sqlalchemy as sa
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import logging

from sqlalchemy.sql.elements import BindParameter, ClauseElement
from sqlalchemy.orm.attributes import QueryableAttribute
from decimal import Decimal

from app.db.model.product import SkuInfo, ProductSyncCandidate, ProductSyncChunk
from app.utils.serialization import format_product_tags


'''
  定义“参与对比/写库的字段白名单”。5个字段
  作用：做 diff 时只比较这些字段，避免无关字段导致“假变更”。
  Upsert 时只更新这些字段，减少写放大。以 sku_info 规范字段为准
'''
SYNC_FIELDS = [
    "sku_code",
    "brand",
    "stock_qty",
    "supplier",
    "ean_code",
    "price",
    "rrp_price",
    "special_price",
    "special_price_end_date",
    "shopify_price",
    "shopify_variant_id",
    "weight",
    "length",
    "width",
    "height",
    "cbm",
    "product_tags",
    "freight_act",
    "freight_nsw_m",
    "freight_nsw_r",
    "freight_nt_m",
    "freight_nt_r",
    "freight_qld_m",
    "freight_qld_r",
    "remote",
    "freight_sa_m",
    "freight_sa_r",
    "freight_tas_m",
    "freight_tas_r",
    "freight_vic_m",
    "freight_vic_r",
    "freight_wa_m",
    "freight_wa_r",
    "freight_nz",
    "attrs_hash_current",
]


# 前端表格用到的主要字段（与 /products 返回一致
_PRODUCT_EXPORT_COLUMNS = [
    "sku_code", "stock_qty", "price", "rrp_price", "special_price", "special_price_end_date",
    "freight_act", "freight_nsw_m", "freight_nsw_r", "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r", "remote", "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r", "freight_vic_m", "freight_vic_r", "freight_wa_m",
    "freight_wa_r", "freight_nz",

    "ean_code", "brand", "supplier", "weight", "length", "width", "height", "cbm",
    "product_tags", "shopify_price", "attrs_hash_current", "updated_at",
]


# # 前端表格用到的主要字段 与 sku_info DB字段映射关系
_PRODUCT_HEADER_LABELS = {
    "sku_code": "SKU",
    "stock_qty": "Stock Qty",
    "price": "price",
    "rrp_price": "RrpPrice",
    "special_price": "Special Price",
    "special_price_end_date": "Special Price End Date",
    
    "freight_act": "ACT",
    "freight_nsw_m": "NSW_M",
    "freight_nsw_r": "NSW_R",
    "freight_nt_m": "NT_M",
    "freight_nt_r": "NT_R",
    "freight_qld_m": "QLD_M",
    "freight_qld_r": "QLD_R",
    "remote": "REMOTE",
    "freight_sa_m": "SA_M",
    "freight_sa_r": "SA_R",
    "freight_tas_m": "TAS_M",
    "freight_tas_r": "TAS_R",
    "freight_vic_m": "VIC_M",
    "freight_vic_r": "VIC_R",
    "freight_wa_m": "WA_M",
    "freight_wa_r": "WA_R",
    "freight_nz": "NZ",
    "ean_code": "EAN Code",
    "brand": "Brand",
    "supplier": "Supplier",
    "weight": "Weight(kg)",
    "length": "Carton Length(cm)",
    "width": "Carton Width(cm)",
    "height": "Carton Height(cm)",
    "cbm": "CBM",
    "product_tags": "Product Tags",
    "shopify_price": "Shopify Price",
    "attrs_hash_current": "attrs hash",
    "updated_at": "Updated At",
}

_PRODUCT_CSV_HEADERS = [
    _PRODUCT_HEADER_LABELS.get(col, col) for col in _PRODUCT_EXPORT_COLUMNS
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



def _clean_row_values(row: dict) -> dict:
    """
    Normalize a snapshot row so it contains plain Python values safe for SQLAlchemy.
    """
    clean: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, BindParameter):
            value = getattr(value, "effective_value", getattr(value, "value", None))

        if _is_sqlalchemy_expression(value):
            logger.warning(
                "bulk payload detected SQL expression: field=%s type=%s value=%r",
                key,
                type(value),
                value,
            )
            raise ValueError(
                f"bulk payload field={key!r} is SQL expression ({type(value)!r}); provide plain Python value"
            )
        
        # if key in {"length", "width", "height", "cbm", "weight"}:
        #     logger.info(
        #         "upsert payload field=%s type=%s value=%r",
        #         key,
        #         type(value),
        #         value,
        #     )
        if isinstance(value, Decimal) and not value.is_finite():
            value = None
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            value = None
        clean[str(key)] = value
    return clean


def _prepare_bulk_payload(rows: list[dict], *, key: str) -> list[dict]:
    """
    Deduplicate/clean rows before bulk persistence.
    """
    deduped: dict[str, dict] = {}
    for row in rows or []:
        identifier = row.get(key)
        if not identifier:
            continue
        deduped[str(identifier)] = _clean_row_values(row)
    return list(deduped.values())


def _execute_upsert(
    db: Session,
    table,
    rows: list[dict],
    *,
    conflict_keys: List[str],
    update_columns: List[str],
    extra_updates: Optional[Dict[str, Any]] = None,
) -> int:
    if not rows:
        return 0
    
    # 不要把冲突键 sku_code 也更新
    update_cols = [c for c in update_columns if c not in conflict_keys]

    # 冲突键转换成真正的 Column 对象
    # conflict_cols = [getattr(table.c, k) for k in conflict_keys]

    chunk_size: int = 1000
    total = 0

    for idx in range(0, len(rows), chunk_size):
        chunk = [_clean_row_values(row) for row in rows[idx : idx + chunk_size]]
        # 构造 INSERT ... VALUES (...)
        stmt = insert(table).values(chunk)

        # 如果触发冲突（sku_code 重复），就用这批行里对应列的值去覆盖旧行，同时把 extra_updates（例如 updated_at、last_changed_at）一起写进去
        updates = {col: getattr(stmt.excluded, col) for col in update_cols}
        # updates = {col: getattr(stmt.excluded, col) for col in update_columns}
        # 冲突时的 SET 子句：用 excluded.xxx 覆盖旧值，但如果这一批传的是 NULL，则保留旧值（coalesce）
        # updates = {
        #     col: sa.func.coalesce(getattr(stmt.excluded, col), getattr(table.c, col))
        #     for col in update_cols
        # }

        # 额外更新（updated_at = now()），这里放 SQL 表达式
        if extra_updates:
            updates.update(extra_updates)
        
        # 真正生成 upsert: 这个流程就是“批量 upsert 模板”——一次构建 SQL、一次执行，多批循环是为了别让单条 SQL 带太多 VALUES
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=conflict_keys,
            set_=updates,
        )
        # print(str(upsert_stmt.compile(dialect=sa.dialects.postgresql.dialect())))

        res = db.execute(upsert_stmt)
        total += int(res.rowcount or 0)
    return total




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
            supplier, 
            ean_code,
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



def bulk_upsert_sku_info(db, rows: list[dict], *, only_update_when_changed: bool=False) -> None:
    if not rows:
        return

    payload_rows = _prepare_bulk_payload(rows, key="sku_code")
    if not payload_rows:
        return

    try:
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

        rows_to_persist: list[dict] = []
        for row in payload_rows:
            sku = row.get("sku_code")
            if not sku:
                continue

            if only_update_when_changed:
                current = existing_map.get(sku)
                if current is not None and all(
                    row.get(field) == current.get(field) for field in SYNC_FIELDS
                ):
                    continue

            rows_to_persist.append(row)

        if rows_to_persist:
            _execute_upsert(
                db,
                SkuInfo,
                rows_to_persist,
                conflict_keys=["sku_code"],
                update_columns=SYNC_FIELDS,
                extra_updates={"updated_at": now_expr, "last_changed_at": now_expr},
            )
    except Exception:
        logger.exception(
            "bulk_upsert_sku_info failed: rows=%d only_update=%s sample=%s",
            len(rows),
            only_update_when_changed,
            rows[:10],
        )
        raise


"""
批量保存变更候选记录
场景：给 orchestration/product_sync 用，保存“本次 run 中字段有变化的 SKU”及其变更字段/新值
    - 将预处理好的候选行批量 upsert 到 product_sync_candidates。
    - 需要调用方提前构造 run_id、sku_code、change_mask、new_snapshot、change_count。
    - 冲突覆盖保证“同 run、同 SKU”只保留最后一次写入
    - new_s: 变化字段子集
    - 把“有变化的 SKU”写入 product_sync_candidates（候选表），给后续 5.3 只处理增量
"""
def save_candidates(db: Session, rows: list[dict]) -> int:  
    if not rows:
        return 0

    try:
        return _execute_upsert(
            db,
            ProductSyncCandidate,
            rows,
            conflict_keys=["run_id", "sku_code"],
            update_columns=["change_mask", "new_snapshot", "change_count"],
            extra_updates={"updated_at": func.now()},
        )
    except Exception:
        logger.exception(
            "save_candidates failed: rows=%d sample=%s",
            len(rows),
            rows[:3],
        )
        raise


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
    *, db=None, sku_prefix: Optional[str] = None, tags_csv: Optional[str] = None,
    prefer_sql: bool = True, flush_bytes: int = 64 * 1024,
):

    return export_products_csv_iter_sql(
        db, sku_prefix=sku_prefix, tags_csv=tags_csv, flush_bytes=flush_bytes
    )




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
        d["product_tags"] = format_product_tags(d.get("product_tags"))
        # special_price_end_date/updated_at 让数据库按默认文本输出（或自行格式化）
        w.writerow([d.get(k) for k in _PRODUCT_EXPORT_COLUMNS])

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
        db.execute(
            select(SkuInfo).where(SkuInfo.sku_code.in_(skus))
        )
        .scalars()
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
            "product_tags": getattr(r, "product_tags", None),

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
