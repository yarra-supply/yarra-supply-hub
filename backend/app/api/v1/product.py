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
    # 现在先用 Mock；切 DB 时把 use_mock=False 即可
    USE_MOCK = True
    from .product import MOCK_PRODUCTS  # 与当前文件里的 MOCK_PRODUCTS 保持一致【:contentReference[oaicite:6]{index=6}】

    gen = export_products_csv_iter(
        db=db,
        use_mock=USE_MOCK,
        mock_rows=MOCK_PRODUCTS if USE_MOCK else None,
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
    data["id"] = str(data.get("id")) if data.get("id") is not None else None
    data["special_price_end_date"] = _as_datetime(data.get("special_price_end_date"))
    data["updated_at"] = _as_datetime(data.get("updated_at"))
    tags = data.pop("product_tags", None) or []
    if not isinstance(tags, list):
        tags = []
    data["tags"] = tags
    return Product(**data)






# ---------- Mock模拟数据（可替换为 DB 查询结果） ----------
# ---------- Mock模拟数据（字段与 sku_info 对齐） ----------
MOCK_PRODUCTS = [
    {
        "id": f"p-{i}",
        "sku_code": sku,
        "title": f"Demo {sku}",
        "brand": "Acme",
        "stock_qty": 10 + i,

        "price": round(19.9 + i, 2),
        "rrp_price": round(29.9 + i, 2),
        "special_price": None if i % 3 else round(17.9 + i, 2),
        "special_price_end_date": None if i % 2 else "2025-09-30T23:59:59",

        "shopify_price": round(21.9 + i, 2),

        # 尺寸重量：演示数据（单位在表设计中定义：cm/kg）
        "length": 20.0 + i,
        "width": 10.0 + i,
        "height": 8.0 + i,
        "weight": 1.2 + i * 0.1,
        "cbm": 0.01 + i * 0.001,

        # 运费相关字段：演示数据
        "freight_act": 5.0 + i,
        "freight_nsw_m": 4.0 + i,
        "freight_nsw_r": 6.0 + i,
        "freight_nt_m": 7.0 + i,
        "freight_nt_r": 9.0 + i,
        "freight_qld_m": 5.5 + i,
        "freight_qld_r": 7.5 + i,
        "remote": 10.0 + i,
        "freight_sa_m": 4.5 + i,
        "freight_sa_r": 6.5 + i,
        "freight_tas_m": 8.0 + i,
        "freight_tas_r": 10.0 + i,
        "freight_vic_m": 4.0 + i,
        "freight_vic_r": 6.0 + i,
        "freight_wa_m": 9.0 + i,
        "freight_wa_r": 11.0 + i,
        "freight_nz": 15.0 + i,

        # 其它
        "shopify_variant_id": f"gid://shopify/ProductVariant/1000{i}",
        "attrs_hash_current": "mockhash123",
        "updated_at": datetime.utcnow().isoformat(),
        "product_tags": ["DropShippingZone", "Home"] if i % 2 == 0 else ["Outdoor"],  # ← 底层统一用 product_tags
        # 对外仍返回 tags 字段（下方 list_products 里会做映射；若你直接返回 dict，也可在此冗余 tags）
        "tags": ["DropShippingZone", "Home"] if i % 2 == 0 else ["Outdoor"],
    }
    for i, sku in enumerate(["V201-001","V201-002","V201-100","V202-001","A100-001","V201-AAA"])
]


# MOCK_PRODUCTS: List[Product] = [
#     Product(
#         id="p-1001",
#         sku_code="V952-AAAA1111",
#         title="Portable Blender A",
#         brand="Yarra",
#         stock_qty=120,
#         status="active",
#         price=39.9,
#         special_price=None,
#         special_price_end_date=None,
#         created_at=datetime.fromisoformat("2025-08-01T09:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-30T10:15:00"),
#         tags=["dsz", "DropShippingZone"],
#     ),
#     Product(
#         id="p-1002",
#         sku_code="V952-BBBB2222",
#         title="Desk Lamp B",
#         brand="Yarra",
#         stock_qty=0,
#         status="active",
#         price=24.9,
#         special_price=19.9,
#         special_price_end_date=datetime.fromisoformat("2025-09-07T23:59:59"),
#         created_at=datetime.fromisoformat("2025-08-05T12:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-28T12:30:00"),
#         tags=["dsz"],
#     ),
#     Product(
#         id="p-1003",
#         sku_code="V952-CCCC3333",
#         title="Vacuum Cleaner C",
#         brand="Yarra",
#         stock_qty=35,
#         status="active",
#         price=129.0,
#         special_price=None,
#         created_at=datetime.fromisoformat("2025-07-25T08:20:00"),
#         updated_at=datetime.fromisoformat("2025-08-27T18:10:00"),
#         tags=["dsz", "home"],
#     ),
#     Product(
#         id="p-1004",
#         sku_code="V952-DDDD4444",
#         title="Smart Kettle D",
#         brand="Yarra",
#         stock_qty=9,
#         status="active",
#         price=59.0,
#         special_price=49.0,
#         special_price_end_date=datetime.fromisoformat("2025-09-02T23:59:59"),
#         created_at=datetime.fromisoformat("2025-08-10T10:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-29T11:30:00"),
#         tags=["dsz", "kitchen", "DropShippingZone"],
#     ),
#     Product(
#         id="p-1005",
#         sku_code="V952-EEEE5555",
#         title="Air Fryer E",
#         brand="Yarra",
#         stock_qty=200,
#         status="active",
#         price=79.0,
#         created_at=datetime.fromisoformat("2025-08-15T14:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-31T09:45:00"),
#         tags=["dsz", "kitchen"],
#     ),
#     Product(
#         id="p-1006",
#         sku_code="V952-FFFF6666",
#         title="Wireless Charger F",
#         brand="Yarra",
#         stock_qty=18,
#         status="active",
#         price=19.9,
#         created_at=datetime.fromisoformat("2025-07-30T07:10:00"),
#         updated_at=datetime.fromisoformat("2025-08-25T15:15:00"),
#         tags=["dsz", "electronics"],
#     ),
#     Product(
#         id="p-1007",
#         sku_code="V952-GGGG7777",
#         title="Camping Light G",
#         brand="Yarra",
#         stock_qty=55,
#         status="active",
#         price=29.9,
#         created_at=datetime.fromisoformat("2025-08-20T09:40:00"),
#         updated_at=datetime.fromisoformat("2025-08-30T20:00:00"),
#         tags=["dsz", "outdoor", "DropShippingZone"],
#     ),
#     Product(
#         id="p-1008",
#         sku_code="V952-HHHH8888",
#         title="Bluetooth Speaker H",
#         brand="Yarra",
#         stock_qty=0,
#         status="discontinued",
#         price=49.9,
#         created_at=datetime.fromisoformat("2025-06-10T09:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-18T09:00:00"),
#         tags=["dsz", "electronics"],
#     ),
#     Product(
#         id="p-1009",
#         sku_code="V952-IIII9999",
#         title="Fitness Band I",
#         brand="Yarra",
#         stock_qty=88,
#         status="active",
#         price=44.0,
#         created_at=datetime.fromisoformat("2025-08-18T12:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-31T07:30:00"),
#         tags=["dsz", "wearable"],
#     ),
#     Product(
#         id="p-1010",
#         sku_code="V952-JJJJ0000",
#         title="Robot Mop J",
#         brand="Yarra",
#         stock_qty=6,
#         status="active",
#         price=189.0,
#         created_at=datetime.fromisoformat("2025-08-22T16:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-31T08:30:00"),
#         tags=["dsz", "home"],
#     ),
#     Product(
#         id="p-1011",
#         sku_code="V952-JJJJ0001",
#         title="Robot Mop JJ",
#         brand="Yarra",
#         stock_qty=6,
#         status="active",
#         price=190.0,
#         created_at=datetime.fromisoformat("2025-08-22T16:00:00"),
#         updated_at=datetime.fromisoformat("2025-08-31T08:30:00"),
#         tags=["dsz", "home"],
#     ),
# ]
