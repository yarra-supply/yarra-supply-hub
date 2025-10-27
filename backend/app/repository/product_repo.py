
# product database repository

from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, Dict, Any, List, Iterator, Tuple
from decimal import Decimal
import io, csv, json
import math
import uuid

import sqlalchemy as sa
from sqlalchemy import select, func, case, tuple_, bindparam, text, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import logging

from sqlalchemy.sql.elements import BindParameter
from decimal import Decimal

from sqlalchemy.exc import CompileError

from app.db.model.product import SkuInfo, ProductSyncCandidate, ProductSyncChunk


'''
 定义“参与对比/写库的字段白名单”。
 作用：做 diff 时只比较这些字段，避免无关字段导致“假变更”。
 Upsert 时只更新这些字段，减少写放大。以 sku_info 规范字段为准
'''
SYNC_FIELDS = [
    "sku_code", "brand", "stock_qty", 
    "price", "rrp_price", "special_price", "special_price_end_date", "shopify_price",
    "weight", "length", "width", "height",
    "product_tags",

    "freight_act", "freight_nsw_m", "freight_nsw_r", "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r", "remote", "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r", "freight_vic_m", "freight_vic_r", "freight_wa_m",
    "freight_wa_r", "freight_nz",
    "attrs_hash_current",           # 新增：后续增量/5.3 计算需要
    # "freight_by_zone",              # 若表里没有该列，后续 upsert 会忽略传值
    # todo 增加其他字段
]


# 前端表格用到的主要字段（与 /products 返回一致
_PRODUCT_EXPORT_COLUMNS = [
    "sku_code", "brand", "stock_qty", 
    "price", "rrp_price", "special_price", "special_price_end_date", "shopify_price",
    "weight", "length", "width", "height", "product_tags",
    "freight_act", "freight_nsw_m", "freight_nsw_r", "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r", "remote", "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r", "freight_vic_m", "freight_vic_r", "freight_wa_m",
    "freight_wa_r", "freight_nz",
    "updated_at", 
]
_PRODUCT_CSV_HEADERS = _PRODUCT_EXPORT_COLUMNS[:]  # 头 = 同名



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
  仅返回 (sku_code, price, special_price)，满足 special_price_end_date <= target_date 的候选。
  流式分页，避免一次性拉大表。
"""
def iter_price_reset_candidates(db: Session, *, target_date, 
                                page_size: int = 1000) -> Iterator[Tuple[str, float, float | None]]:
   
    offset = 0
    sql = text("""
        SELECT sku_code, price, special_price
        FROM sku_info
        WHERE special_price_end_date IS NOT NULL
          AND special_price_end_date <= :target_date
        ORDER BY sku_code
        LIMIT :limit OFFSET :offset
    """)

    while True:
        rows = db.execute(sql, {"target_date": target_date, "limit": page_size, "offset": offset}).mappings().all()
        if not rows:
            break
        for r in rows:
            yield (
                r["sku_code"],
                float(r["price"]),
                (float(r["special_price"]) if r["special_price"] is not None else None),
            )
        offset += page_size



# ========= 对比新旧快照，找出变更字段 =========
# 场景：给 orchestration/product_sync 用，找出有变化的 SKU 及其变化字段
# 一次对比一个sku
def diff_snapshot(old: dict|None, new: dict) -> dict:

    # 先比对 attrs_hash_current, 若新旧哈希一致，则直接跳过逐字段对比
    base = old or {}
    if base.get("attrs_hash_current") == new.get("attrs_hash_current"):
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
    # 传入的 rows 建议本身就是“只包含变更”的子集（上游已筛过），此处再加一层 where(changed) 保险。
    if not rows: 
        return
    
    # todo 当前单条更新了
    for idx, row in enumerate(rows):
        if "length" in row:
            print(
                "[bulk_upsert_sku_info] row", idx, "length", row["length"], "type", type(row["length"])
            )
        for key, value in row.items():
            if isinstance(value, BindParameter):
                print(
                    "[bulk_upsert_sku_info] BindParameter detected row",
                    idx,
                    "column",
                    key,
                    "value",
                    value,
                )
                raise ValueError(
                    f"row {idx} field {key} is BindParameter; provide plain Python value"
                )
        stmt = insert(SkuInfo).values(row)

        excluded_tuple = tuple_(*[getattr(stmt.excluded, c) for c in SYNC_FIELDS])
        current_tuple  = tuple_(*[getattr(SkuInfo, c) for c in SYNC_FIELDS])
        changed = current_tuple.is_distinct_from(excluded_tuple)

        updates = {c: getattr(stmt.excluded, c) for c in SYNC_FIELDS}

        if only_update_when_changed:
            updates.update({"updated_at": func.now(), "last_changed_at": func.now()})
            stmt = stmt.on_conflict_do_update(
                index_elements=[SkuInfo.sku_code],
                set_=updates,
                where=changed,
            )
        else:
            updates.update({
                "updated_at": func.now(),
                "last_changed_at": case((changed, func.now()), else_=SkuInfo.last_changed_at),
            })
            stmt = stmt.on_conflict_do_update(
                index_elements=[SkuInfo.sku_code],
                set_=updates
            )

        db.execute(stmt)




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
    
    rows = []
    seen = set()    # 同一次调用内的去重
    
    # def _jsonify(value, path=None):
    #     path = path or []
    #     if isinstance(value, Decimal):
    #         return float(value)
    #     if isinstance(value, (datetime, date)):
    #         return value.isoformat()
    #     if isinstance(value, dict):
    #         return {k: _jsonify(v, path + [k]) for k, v in value.items()}
    #     if isinstance(value, list):
    #         return [_jsonify(v, path + [str(i)]) for i, v in enumerate(value)]
    #     return value
    

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
        print("[save_candidates] sku", sku, "snapshot", snapshot)
        
        rows.append({
            "run_id": run_id,
            "sku_code": sku,
            "change_mask": change_mask,  # jsonb object
            "new_snapshot": snapshot,       # 就是 new_s 本身，保存变化字段的新值，便于下游有选择地处理（比如只看价格变化）
            "change_count": change_count,
        })

    if not rows:
        return 0

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
            freight_act, freight_nsw_m, freight_nsw_r, freight_qld_m, freight_qld_r,
            freight_sa_m, freight_sa_r, freight_tas_m, freight_tas_r,
            freight_vic_m, freight_vic_r, freight_wa_m, freight_wa_r,
            remote, freight_nz,
            weight
        FROM sku_info
        WHERE sku_code = ANY(:skus)
    """)
    rows = db.execute(sql, {"skus": skus}).mappings().all()
    return {r["sku_code"]: dict(r) for r in rows}





# ---------------- Manifest 封装（pending / running / succeeded / failed）---------------- #
def upsert_chunk_pending(db: Session, run_id: str, chunk_idx: int, sku_codes: list[str]) -> None:  # [NEW]
    """切片时写入/刷新 manifest 为 pending"""
    if not sku_codes:
        sku_codes = []
    stmt = insert(ProductSyncChunk).values({
        "run_id": run_id,
        "chunk_idx": chunk_idx,
        "status": "pending",
        "sku_codes": sku_codes,
        "sku_count": len(sku_codes),
        "dsz_missing": 0, "dsz_failed_batches": 0, "dsz_failed_skus": 0,
        "dsz_requested_total": 0, "dsz_returned_total": 0,
    }).on_conflict_do_update(
        index_elements=["run_id", "chunk_idx"],
        set_={
            "status": "pending",
            "sku_codes": sa.literal(sku_codes),
            "sku_count": len(sku_codes),
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
def mark_chunk_succeeded(db: Session, run_id: str, idx: int, stats: dict) -> None: 
    updates = {
        "status": "succeeded",
        "finished_at": func.now(),
        "dsz_missing": int(stats.get("missing_count", 0)),
        "dsz_failed_batches": int(stats.get("failed_batches_count", 0)),
        "dsz_failed_skus": int(stats.get("failed_skus_count", 0)),
        "dsz_requested_total": int(stats.get("requested_total", 0)),
        "dsz_returned_total": int(stats.get("returned_total", 0)),
    }

    # 明细列表（若模型包含这些列则写入；否则忽略）
    if hasattr(ProductSyncChunk, "dsz_missing_sku_list"):
        updates["dsz_missing_sku_list"] = stats.get("missing_sku_list", [])
    if hasattr(ProductSyncChunk, "dsz_failed_sku_list"):
        updates["dsz_failed_sku_list"] = stats.get("failed_sku_list", [])
    if hasattr(ProductSyncChunk, "dsz_extra_sku_list"):
        updates["dsz_extra_sku_list"] = stats.get("extra_sku_list", [])

    db.execute(
        sa.update(ProductSyncChunk)
        .where(ProductSyncChunk.run_id == run_id, ProductSyncChunk.chunk_idx == idx)
        .values(**updates)
    )


def mark_chunk_failed(db: Session, run_id: str, idx: int, err: Exception|str) -> None:  # [NEW]
    db.execute(
        sa.update(ProductSyncChunk)
        .where(ProductSyncChunk.run_id==run_id, ProductSyncChunk.chunk_idx==idx)
        .values(status="failed", finished_at=func.now(), last_error=str(err)[:2000])
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
        # tags 序列化成人类可读 JSON
        if isinstance(row.get("tags"), (list, dict)):
            row["tags"] = json.dumps(row["tags"], ensure_ascii=False)
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

    # status 从 stock_qty 推导（>0 则 active，否则 discontinued）
    sql = f"""
      SELECT sku_code, brand, stock_qty,
             CASE WHEN COALESCE(stock_qty,0) > 0 THEN 'active' ELSE 'discontinued' END AS status,
             price, special_price, special_price_end_date,
             updated_at, product_tags AS tags
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
        if isinstance(d.get("tags"), (list, dict)):
            d["tags"] = json.dumps(d["tags"], ensure_ascii=False)
        # special_price_end_date/updated_at 让数据库按默认文本输出（或自行格式化）
        w.writerow([d.get(k) for k in _PRODUCT_CSV_HEADERS])

        if buf.tell() >= flush_bytes:
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    left = buf.getvalue()
    if left:
        yield left




# ==== Price Reset: 候选集流式读取（special 结束日 = target_date） ====
"""
    流式分页返回需要“price 还原”的候选 (sku_code, shopify_variant_id, price)
    选择条件：
      - special_price_end_date == target_date
      - shopify_variant_id is not null
      - price is not null
    返回的 variant_id 是数据库中存储的原值（可能是裸 ID 或 GID）,
    GID 化请在上层做（编排层更清楚外部系统需要的格式）。
"""
def iter_price_reset_candidates(
    db: Session,
    *,
    target_date,              # 明天的日期（date）
    page_size: int = 1000,    # 分页大小（5k 数据建议 500~2000）
) -> Iterator[Tuple[str, str, Decimal]]:

    offset = 0
    while True:
        rows = db.execute(
            select(SkuInfo.sku_code, SkuInfo.shopify_variant_id, SkuInfo.price)
            .where(
                and_(
                    SkuInfo.special_price_end_date <= target_date,
                    SkuInfo.shopify_variant_id.isnot(None),
                    SkuInfo.price.isnot(None),
                )
            )
            .order_by(SkuInfo.sku_code)  # 稳定分页
            .limit(page_size)
            .offset(offset)
        ).all()

        if not rows:
            break

        for sku, vid, price in rows:
            # 统一 Decimal；None 已在 where 里过滤
            yield sku, str(vid), Decimal(str(price))

        if len(rows) < page_size:
            break
        offset += page_size



"""
 查询sku_info表，获取产品相关信息（提供给kogan template流程）
     - 读取产品信息，返回 {sku: 字段字典}，字段名尽量与 service 映射需要的 keys 对齐。
     - 这里采用“宽表式”安全取值：即使某些列不存在也不报错（getattr 兜底 None）。
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
            "price": getattr(r, "price", None),
            "rrp": getattr(r, "rrp", None),
            "kogan_first_price": getattr(r, "kogan_first_price", None),
            "handling_days": getattr(r, "handling_days", None),
            "barcode": getattr(r, "barcode", None),
            "stock": getattr(r, "stock", None),
            "weight": getattr(r, "weight", None),
            "brand": getattr(r, "brand", None),
            "title": getattr(r, "title", None),
            "description": getattr(r, "description", None),
            "subtitle": getattr(r, "subtitle", None),
            "whats_in_the_box": getattr(r, "whats_in_the_box", None),
            "category": getattr(r, "category", None),
        }
    return out
logger = logging.getLogger(__name__)
