# 产品相关接口 -> 前端产品页面调用查询

from __future__ import annotations
from logging import log
from fastapi import APIRouter, Query, Depends, Response
from fastapi.responses import StreamingResponse
from typing import List, Optional, Literal, Dict, Any
from datetime import datetime, date, time, timezone
from decimal import Decimal
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from pydantic import BaseModel, Field
import logging
from app.repository.product_repo import (
    export_products_csv_iter,
    fetch_distinct_product_tags,
    fetch_products_page,
)
from app.services.auth_service import get_current_user



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["products"],
    dependencies=[Depends(get_current_user)], 
)


# ---------- Pydantic 模型（返回结构更清晰，OpenAPI 也更友好） ----------
class Product(BaseModel):
    id: str
    sku_code: str
    # title: Optional[str] = None
    brand: Optional[str] = None
    stock_qty: Optional[int] = None

    price: Optional[float] = None
    rrp_price: Optional[float] = None
    special_price: Optional[float] = None
    special_price_end_date: Optional[datetime] = None
    shopify_price: Optional[float] = None

    # 新增：尺寸重量（与 sku_info 对齐）
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    weight: Optional[float] = None
    cbm: Optional[float] = None
    supplier: Optional[str] = None
    ean_code: Optional[str] = None

    # 运费相关字段
    freight_act: Optional[float] = None
    freight_nsw_m: Optional[float] = None
    freight_nsw_r: Optional[float] = None
    freight_nt_m: Optional[float] = None
    freight_nt_r: Optional[float] = None
    freight_qld_m: Optional[float] = None
    freight_qld_r: Optional[float] = None
    remote: Optional[float] = None
    freight_sa_m: Optional[float] = None
    freight_sa_r: Optional[float] = None
    freight_tas_m: Optional[float] = None
    freight_tas_r: Optional[float] = None
    freight_vic_m: Optional[float] = None
    freight_vic_r: Optional[float] = None
    freight_wa_m: Optional[float] = None
    freight_wa_r: Optional[float] = None
    freight_nz: Optional[float] = None

    # 新增：其余与表一致的字段
    shopify_variant_id: Optional[str] = None
    attrs_hash_current: Optional[str] = None

    updated_at: Optional[datetime] = None

    # tags 依旧对外暴露为 tags；底层来自 sku_info.product_tags
    tags: Optional[List[str]] = None


class ProductsPage(BaseModel):
    items: List[Product]
    total: int



# ---------- 依赖 ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



@router.get("/products/tags", response_model=List[str])
def list_product_tags(db: Session = Depends(get_db)):
    tags = fetch_distinct_product_tags(db)
    return tags



"""
    商品分页查询(Mock 数据）
    - 支持: sku / tag / created_from / created_to
    - 分页: page / page_size(也兼容 size)
    设计依据: PDF 5.2(数据量4W、服务端分页、筛选条件)。contentReference[oaicite:1]{index=1}
    """
@router.get("/products", response_model=ProductsPage)
def list_products(
    sku: Optional[str] = Query(None, description="SKU 前缀（如 V201-；前缀匹配）"),
    tag: Optional[str] = Query(None, description="按 tag 精确匹配（出现在 tags 数组中）"),
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: Optional[int] = Query(None, alias="page_size", ge=1, le=200, description="每页条数"),
    size: Optional[int] = Query(None, alias="size", ge=1, le=200, description="兼容旧参数名 size"),
    db: Session = Depends(get_db),
):
    
    logger.info("products: sku=%s tag=%s page=%s page_size=%s",sku, tag, page, page_size or size)

    # 兼容 page_size 与 size 两个参数名
    ps = page_size or size or 20

    tags_filter = _normalize_tags_filter(tag)
    rows, total = fetch_products_page(
        db,
        sku_prefix=sku,
        tags=tags_filter,
        page=page,
        page_size=ps,
    )

    items = [_build_product_from_row(row) for row in rows]

    logger.info("products: total=%s return=%s", total, len(items))
    return ProductsPage(items=items, total=total)




'''
  商品列表导出为 CSV
'''
@router.get("/products/export")
def export_products_csv(
    response: Response,
    sku: Optional[str] = None,
    tag: Optional[str] = None,
    db: Session = Depends(get_db),
):

    gen = export_products_csv_iter(
        db=db,
        sku_prefix=sku,
        tags_csv=tag,
        prefer_sql=True,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f'products_{ts}.csv'

    return StreamingResponse(
        gen,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            # 跨域时让前端 JS 能读到文件名（如果全局 CORS 已 expose，也可以不写这行）
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )




# ---------- 工具 ----------
def _to_date(dt: Optional[datetime]) -> Optional[date]:
    return dt.date() if isinstance(dt, datetime) else None


def _normalize_tags_filter(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    items = [t.strip() for t in raw.split(",") if t.strip()]
    return items or None


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time())
    return None


_DECIMAL_FIELDS = [
    "price",
    "rrp_price",
    "special_price",
    "shopify_price",
    "weight",
    "length",
    "width",
    "height",
    "cbm",
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
]


def _build_product_from_row(row: Dict[str, Any]) -> Product:
    data = dict(row)
    for key in _DECIMAL_FIELDS:
        if key in data:
            data[key] = _as_float(data.get(key))
    data["supplier"] = row.get("supplier")
    data["ean_code"] = row.get("ean_code")
    data["id"] = str(data.get("id")) if data.get("id") is not None else None
    data["special_price_end_date"] = _as_datetime(data.get("special_price_end_date"))
    data["updated_at"] = _as_datetime(data.get("updated_at"))
    tags = data.pop("product_tags", None) or []
    if not isinstance(tags, list):
        tags = []
    data["tags"] = tags
    return Product(**data)
