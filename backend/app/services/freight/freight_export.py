
from __future__ import annotations

import io
import csv
import json
from typing import List, Dict, Optional, Iterable, Any, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


# —— 约定导出列（与前端展示一致）；把 update_at 起别名为 updated_at，tags 用 product_tags 起别名 —— #
_EXPORT_COLUMNS_SQL = [
    "sku_code",
    "shipping_type",
    "adjust", "same_shipping", "shipping_ave", "shipping_ave_m", "shipping_ave_r", "shipping_med",
    "shipping_med_dif", "rural_ave", "weighted_ave_s", "cubic_weight", "remote_check", "cost",
    "selling_price", "shopify_price", "kogan_au_price", "kogan_k1_price", "kogan_nz_price",
    "product_tags AS tags",
    "update_at AS updated_at",
]

# CSV 头（把 AS 后面的别名作为列名）
_CSV_HEADERS = [c.split(" AS ")[-1] if " AS " in c else c for c in _EXPORT_COLUMNS_SQL]



# ============= 导出运费结果CSV文件：根据开关选择 DB / Mock ============= #
def export_freight_csv_iter(
    *,
    db: Optional[Session],
    use_mock: bool,
    mock_rows: Optional[Iterable[Dict[str, Any]]] = None,
    sku_prefix: Optional[str] = None,
    tags_csv: Optional[str] = None,
    shipping_types_csv: Optional[str] = None,
    flush_bytes: int = 64 * 1024,
):
    """
    统一的导出生成器：
      - use_mock=True：从 mock_rows 输出（db 可为 None）
      - use_mock=False：从 DB 输出（db 必须可用），prefer_sql 控制 SQL/ORM
    """
    if use_mock:
        if mock_rows is None:
            raise ValueError("use_mock=True 但 mock_rows 未提供")
        return export_freight_csv_iter_mock(mock_rows, flush_bytes=flush_bytes, 
                                            sku_prefix=sku_prefix,
                                            tags_csv=tags_csv,
                                            shipping_types_csv=shipping_types_csv,)

    if db is None:
        raise ValueError("use_mock=False 需要传入有效的 db 会话")

    return export_freight_csv_iter_sql(
        db,
        sku_prefix=sku_prefix,
        tags_csv=tags_csv,
        shipping_types_csv=shipping_types_csv,
        flush_bytes=flush_bytes,
    )



# ============= Mock 数据导出（现在先用它跑通） ============= #
"""
    生成器：把内存中的 mock 列表（List[dict]）导出为 CSV。
    要求 rows 的 key 包含 _CSV_HEADERS 对应列；缺失的填 None。
"""
def export_freight_csv_iter_mock(
    rows: Iterable[Dict[str, Any]],
    *,
    flush_bytes: int = 64 * 1024, 
    sku_prefix: Optional[str] = None,
    tags_csv: Optional[str] = None,
    shipping_types_csv: Optional[str] = None,
):

    # ★ 按条件在内存里过滤
    data = _filter_rows_in_memory(rows, sku_prefix, tags_csv, shipping_types_csv)

    buf = io.StringIO()
    writer = csv.writer(buf)

    writer.writerow(_CSV_HEADERS)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)

    for r in data:
        d = {}
        # 兼容 mock 的字段命名（如果是 product_tags / update_at，起别名成 tags/updated_at）
        for h in _CSV_HEADERS:
            if h == "tags":
                v = r.get("tags", r.get("product_tags"))
                d[h] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
            elif h == "updated_at":
                d[h] = r.get("updated_at", r.get("update_at"))
            else:
                d[h] = r.get(h)
        writer.writerow([d.get(h) for h in _CSV_HEADERS])

        payload = buf.getvalue()
        if payload and len(payload) >= flush_bytes:
            yield payload
            buf.seek(0); buf.truncate(0)

    leftover = buf.getvalue()
    if leftover:
        yield leftover


# 内存过滤（仅供 Mock 用）
def _filter_rows_in_memory(
    rows: Iterable[Dict[str, Any]],
    sku_prefix: Optional[str],
    tags_csv: Optional[str],
    shipping_types_csv: Optional[str],
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
                if any((t or "").lower() in wanted for t in (r.get("tags") or []))
            ]

    if shipping_types_csv:
        st = {s.strip() for s in shipping_types_csv.split(",") if s.strip()}
        if st:
            data = [r for r in data if (r.get("shipping_type") or "") in st]

    return data


# ============= 方式 B：原生 SQL + 流式（推荐） ============= #
def export_freight_csv_iter_sql(
    db: Session,
    *,
    sku_prefix: Optional[str],
    tags_csv: Optional[str],
    shipping_types_csv: Optional[str],
    flush_bytes: int = 64 * 1024,
):
    """
    生成器：从 DB 按条件读取，流式写 CSV（原生 SQL）。
    用法（在路由里）：StreamingResponse(export_freight_csv_iter_sql(...), media_type='text/csv')
    """
    where_sql, params = _build_where_sql_for_export(sku_prefix, tags_csv, shipping_types_csv)
    sql = f"""
        SELECT {", ".join(_EXPORT_COLUMNS_SQL)}
        FROM kogan_sku_freight_fee
        WHERE {where_sql}
        ORDER BY sku_code
    """
    conn = db.connection()
    rs = conn.execution_options(stream_results=True).execute(text(sql), params)

    buf = io.StringIO()
    writer = csv.writer(buf)

    # 1) 头
    writer.writerow(_CSV_HEADERS)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)

    keys = rs.keys()
    for row in rs:
        d = dict(zip(keys, row))
        # tags 是 JSONB，序列化成 JSON 字符串，避免 CSV 变形
        if isinstance(d.get("tags"), (list, dict)):
            d["tags"] = json.dumps(d["tags"], ensure_ascii=False)
        writer.writerow([d.get(h) for h in _CSV_HEADERS])
        # 分块 flush，保证长流稳定
        for chunk in _csv_write_flush(writer, buf, flush_bytes):  # type: ignore
            yield chunk

    # 尾块
    leftover = buf.getvalue()
    if leftover:
        yield leftover


# 构建导出用的 where 子句（原生 SQL 版）
def _build_where_sql_for_export(
    sku_prefix: Optional[str],
    tags_csv: Optional[str],
    shipping_types_csv: Optional[str],
) -> tuple[str, dict]:

    conds, params = ["1=1"], {}

    if sku_prefix:
        conds.append("sku_code ILIKE :sku")
        params["sku"] = sku_prefix + "%"

    if shipping_types_csv:
        sts = [s.strip() for s in shipping_types_csv.split(",") if s.strip()]
        if sts:
            ph = []
            for i, v in enumerate(sts):
                k = f"st{i}"
                params[k] = v
                ph.append(f":{k}")
            conds.append(f"shipping_type IN ({','.join(ph)})")

    if tags_csv:
        tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
        if tags:
            ph = []
            for i, v in enumerate(tags):
                k = f"tag{i}"
                params[k] = v
                ph.append(f":{k}")
            # JSONB: “任意一个 tag 命中”
            conds.append(f"product_tags ?| ARRAY[{','.join(ph)}]")

    return " AND ".join(conds), params



"""把缓冲区内容吐出去，然后清空。"""
def _csv_write_flush(writer: csv.writer, buf: io.StringIO, flush_bytes: int):
    payload = buf.getvalue()
    if payload and len(payload) >= flush_bytes:
        yield payload
        buf.seek(0); buf.truncate(0)