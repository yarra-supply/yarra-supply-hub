
from __future__ import annotations

import io
import csv
import json
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


# —— 约定导出列（与前端展示一致） —— #
_EXPORT_COLUMNS_SQL = [
    "f.sku_code",
    "f.shipping_type",
    "f.adjust",
    "f.same_shipping",
    "f.shipping_ave",
    "f.shipping_ave_m",
    "f.shipping_ave_r",
    "f.shipping_med",
    "f.remote_check",
    "f.rural_ave",
    "f.weighted_ave_s",
    "f.shipping_med_dif",
    "f.cubic_weight",
    "f.weight",
    "f.price_ratio",
    "f.selling_price",
    "f.shopify_price",
    "f.kogan_au_price",
    "f.kogan_k1_price",
    "f.kogan_nz_price",
    "COALESCE(si.product_tags, '[]'::jsonb) AS product_tags",
    "si.price AS cost",
    "f.updated_at",
]

# CSV 头（把 AS 后面的别名作为列名）
_CSV_HEADERS = [c.split(" AS ")[-1] if " AS " in c else c.split(".")[-1] for c in _EXPORT_COLUMNS_SQL]


# ============= 原生 SQL + 流式导出 ============= #
def export_freight_csv_iter(
    db: Session,
    *,
    sku_prefix: Optional[str],
    tags_csv: Optional[str],
    shipping_types_csv: Optional[str],
    flush_bytes: int = 64 * 1024,
):
    if db is None:
        raise ValueError("导出运费结果需要有效的数据库会话")

    return export_freight_csv_iter_sql(
        db,
        sku_prefix=sku_prefix,
        tags_csv=tags_csv,
        shipping_types_csv=shipping_types_csv,
        flush_bytes=flush_bytes,
    )


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
        FROM kogan_sku_freight_fee AS f
        LEFT JOIN sku_info AS si ON si.sku_code = f.sku_code
        WHERE {where_sql}
        ORDER BY f.sku_code
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
        if isinstance(d.get("product_tags"), (list, dict)):
            d["product_tags"] = json.dumps(d["product_tags"], ensure_ascii=False)
        updated = d.get("updated_at")
        if isinstance(updated, datetime):
            d["updated_at"] = updated.replace(microsecond=0).isoformat()
        writer.writerow([d.get(h) for h in _CSV_HEADERS])
        # 分块 flush，保证长流稳定
        for chunk in _csv_write_flush(buf, flush_bytes):
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
        conds.append("f.sku_code ILIKE :sku")
        params["sku"] = sku_prefix + "%"

    if shipping_types_csv:
        sts = [s.strip() for s in shipping_types_csv.split(",") if s.strip()]
        if sts:
            ph = []
            for i, v in enumerate(sts):
                k = f"st{i}"
                params[k] = v
                ph.append(f":{k}")
            conds.append(f"f.shipping_type IN ({','.join(ph)})")

    if tags_csv:
        tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
        if tags:
            ph = []
            for i, v in enumerate(tags):
                k = f"tag{i}"
                params[k] = v
                ph.append(f":{k}")
            # JSONB: “任意一个 tag 命中”
            conds.append(f"COALESCE(si.product_tags, '[]'::jsonb) ?| ARRAY[{','.join(ph)}]")

    return " AND ".join(conds), params



"""把缓冲区内容吐出去，然后清空。"""
def _csv_write_flush(buf: io.StringIO, flush_bytes: int):
    payload = buf.getvalue()
    if payload and len(payload) >= flush_bytes:
        yield payload
        buf.seek(0); buf.truncate(0)
