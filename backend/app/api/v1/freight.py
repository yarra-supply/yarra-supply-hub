# 运费相关接口 -> 前端产品页面调用

from __future__ import annotations
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
from decimal import Decimal
from fastapi import APIRouter, Query, Depends, Response
from starlette.responses import StreamingResponse

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from pydantic import BaseModel
from app.services.freight.freight_export import export_freight_csv_iter
from app.services.auth_service import get_current_user
from app.repository.freight_repo import fetch_shipping_types as repo_fetch_shipping_types, fetch_freight_results_page


router = APIRouter(
    tags=["freight"], 
    dependencies=[Depends(get_current_user)], 
)


class FreightRow(BaseModel):
    id: str
    sku_code: str

    # 基础统计
    adjust: Optional[float] = None
    same_shipping: Optional[float] = None
    shipping_ave: Optional[float] = None

    shipping_ave_m: Optional[float] = None
    shipping_ave_r: Optional[float] = None
    shipping_med: Optional[float] = None
    remote_check: Optional[bool] = None

    rural_ave: Optional[float] = None
    weighted_ave_s: Optional[float] = None
    shipping_med_dif: Optional[float] = None
    
    weight: Optional[float] = None          # 重新计算weight, 公式进行计算, 结果用于更新metafields的 + 添加到kogan上传表格上面 
    cubic_weight: Optional[float] = None
    shipping_type: str                     # '0','1','10','15','20','Extra2','Extra3','Extra4','Extra5'…
    price_ratio: Optional[float] = None

    # 定价结果
    selling_price: Optional[float] = None
    shopify_price: Optional[float] = None
    kogan_au_price: Optional[float] = None
    kogan_k1_price: Optional[float] = None
    kogan_nz_price: Optional[float] = None

    # 标签与时间
    tag: Optional[str] = None
    tags: Optional[List[str]] = None
    updated_at: Optional[str] = None


class FreightPage(BaseModel):
    items: List[FreightRow]
    total: int


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()




@router.get("/freight/shipping-types", response_model=List[str])
def get_shipping_types(db: Session = Depends(get_db)):
    return repo_fetch_shipping_types(db)


@router.get("/freight/results", response_model=FreightPage)
def list_freight(
    sku: Optional[str] = Query(None, description="SKU 前缀（如 V201-；前缀匹配）"),
    tag: Optional[str] = Query(None, description="产品标签, 支持逗号分隔多个(tags 多选会拼接成逗号)"),
    # 兼容两种命名，前端会传 shipping_type（新的多选），也可能传 shippingType（历史）
    shipping_type: Optional[str] = Query(None, alias="shipping_type", description="运费类型，逗号分隔的多个值"),
    shipping_type_camel: Optional[str] = Query(None, alias="shippingType", description="兼容 camelCase"),
    page: int = 1,
    page_size: int = 20,
    db: Session = Depends(get_db),
):
    # clamp page_size 到 1..50
    page_size = max(1, min(page_size, 50))
    st_raw = shipping_type or shipping_type_camel
    shipping_types = _parse_csv_list(st_raw)
    tags_filter = _parse_csv_list(tag)

    rows, total = fetch_freight_results_page(
        db,
        sku_prefix=sku,
        tags=tags_filter,
        shipping_types=shipping_types,
        page=max(page, 1),
        page_size=page_size,
    )

    items = [_build_freight_row(row) for row in rows]
    return FreightPage(items=items, total=total)




''' 
  CSV 导出接口（与表格相同的筛选参数；不分页，导出全部）
  以流式方式写出，支持 4 万+ 行。
  逻辑与列表接口完全一致（同一个函数里），因此导出的数据与表格筛选结果一致
'''
@router.get("/freight/results/export")
def export_freight_results(
    response: Response,
    sku: str | None = None,
    tag: str | None = None,                   # 多选：逗号分隔
    shipping_type: str | None = None,         # 兼容 snake_case
    shippingType: str | None = None,          # 兼容 camelCase
    db: Session = Depends(get_db),
):
    st_raw = shipping_type or shippingType

    gen = export_freight_csv_iter(
        db=db,
        sku_prefix=sku,
        tags_csv=tag,
        shipping_types_csv=st_raw,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f'freight_results_{ts}.csv'

    return StreamingResponse(
        gen,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )





def _parse_csv_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item and item.strip()]


def _decimal_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _format_datetime(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    return None


def _build_freight_row(row: Dict[str, Any]) -> FreightRow:
    tags = row.get("product_tags") or []
    if isinstance(tags, str):
        tags = [tags]

    shipping_type = row.get("shipping_type") or ""
    updated_at = _format_datetime(row.get("updated_at"))

    return FreightRow(
        id=row.get("sku_code", ""),

        sku_code=row.get("sku_code", ""),
        adjust=_decimal_to_float(row.get("adjust")),
        same_shipping=_decimal_to_float(row.get("same_shipping")),
        shipping_ave=_decimal_to_float(row.get("shipping_ave")),

        shipping_ave_m=_decimal_to_float(row.get("shipping_ave_m")),
        shipping_ave_r=_decimal_to_float(row.get("shipping_ave_r")),
        shipping_med=_decimal_to_float(row.get("shipping_med")),
        remote_check=row.get("remote_check"),

        rural_ave=_decimal_to_float(row.get("rural_ave")),
        weighted_ave_s=_decimal_to_float(row.get("weighted_ave_s")),
        shipping_med_dif=_decimal_to_float(row.get("shipping_med_dif")),
        
        weight=_decimal_to_float(row.get("weight")),
        cubic_weight=_decimal_to_float(row.get("cubic_weight")),
        shipping_type=shipping_type,
        price_ratio=_decimal_to_float(row.get("price_ratio")),

        selling_price=_decimal_to_float(row.get("selling_price")),
        shopify_price=_decimal_to_float(row.get("shopify_price")),
        kogan_au_price=_decimal_to_float(row.get("kogan_au_price")),
        kogan_k1_price=_decimal_to_float(row.get("kogan_k1_price")),
        kogan_nz_price=_decimal_to_float(row.get("kogan_nz_price")),

        tag=(tags[0] if tags else None),
        tags=tags,
        updated_at=updated_at,
    )
