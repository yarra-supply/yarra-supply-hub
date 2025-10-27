# 运费相关接口 -> 前端产品页面调用

from __future__ import annotations
from typing import List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Query, Depends, Response
from starlette.responses import StreamingResponse
import csv, io

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from pydantic import BaseModel
from app.services.freight.freight_export import export_freight_csv_iter  # 路径按你的项目层级调整
from app.services.auth_service import get_current_user


router = APIRouter(
    tags=["freight"], 
    dependencies=[Depends(get_current_user)], 
)


class FreightRow(BaseModel):
    id: str
    sku_code: str
    shipping_type: str                     # '0','1','10','15','20','Extra2','Extra3','Extra4','Extra5'…
    zone: Optional[str] = None             # 行键兼容：rowKey 会拼上 zone
    update_weight: Optional[float] = None  # 重新计算weight, 公式进行计算, 结果用于更新metafields的 + 添加到kogan上传表格上面 
    
    # 基础统计
    adjust: Optional[float] = None
    same_shipping: Optional[float] = None
    shipping_ave: Optional[float] = None
    shipping_ave_m: Optional[float] = None
    shipping_ave_r: Optional[float] = None
    shipping_med: Optional[float] = None
    shipping_med_dif: Optional[float] = None
    rural_ave: Optional[float] = None
    weighted_ave_s: Optional[float] = None
    cubic_weight: Optional[float] = None
    remote_check: Optional[bool] = None
    cost: Optional[float] = None
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
def get_shipping_types():
    # 从已有 MOCK 中去重返回（与你现有 FREIGHT_RESULTS 同源）
    return sorted({r.get("shipping_type", "") for r in FREIGHT_RESULTS if r.get("shipping_type")})


@router.get("/freight/results", response_model=FreightPage)
def list_freight(
    sku: Optional[str] = Query(None, description="SKU 前缀（如 V201-；前缀匹配）"),
    tag: Optional[str] = Query(None, description="产品标签, 支持逗号分隔多个(tags 多选会拼接成逗号)"),
    # 兼容两种命名，前端会传 shipping_type（新的多选），也可能传 shippingType（历史）
    shipping_type: Optional[str] = Query(None, alias="shipping_type", description="运费类型，逗号分隔的多个值"),
    shipping_type_camel: Optional[str] = Query(None, alias="shippingType", description="兼容 camelCase"),
    page: int = 1,
    page_size: int = 20,
):
    # clamp page_size 到 1..50
    page_size = max(1, min(page_size, 50))

    data = FREIGHT_RESULTS

    if sku:
        p = sku.lower()
        data = [r for r in data if (r["sku_code"] or "").lower().startswith(p)]

    # 标签多选过滤（逗号分隔）todo 支持多选？
    if tag:
        wanted = {t.strip().lower() for t in tag.split(",") if t.strip()}
        if wanted:
            data = [
                r for r in data
                if any((t or "").lower() in wanted for t in (r.get("tags") or []))
            ]

    # ShippingType 多选过滤（兼容两种命名）
    st_raw = shipping_type or shipping_type_camel
    if st_raw:
        st_set = {s.strip() for s in st_raw.split(",") if s.strip()}
        if st_set:
            data = [r for r in data if (r.get("shipping_type") or "") in st_set]


    total = len(data)
    start = max(0, (page - 1) * page_size)
    end = start + page_size
    return FreightPage(items=data[start:end], total=total)



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
    # —— 现在：用 mock；以后改成 use_mock=False 即切到 DB —— #
    USE_MOCK = True         # <<< 现在先用 mock

    st_raw = shipping_type or shippingType

    gen = export_freight_csv_iter(
        db=db,
        use_mock=USE_MOCK,
        mock_rows=FREIGHT_RESULTS if USE_MOCK else None,
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



# —— Mock 运费结果 ——
# ===================== 15 条 Mock 数据 =====================
_now = datetime.utcnow().replace(microsecond=0).isoformat()

def _mk(i: int, sku: str, st: str, zone: str, base: float, tag: str) -> dict:
    # 用 base 生成一组有内在关系的数值，便于页面观感
    ave   = round(base * 1.02, 2)
    ave_m = round(base * 1.01, 2)
    ave_r = round(base * 1.03, 2)
    med   = round(base, 2)
    same  = round(abs(ave_m - ave_r), 2)
    rural = round(base * 0.15 + 5, 2)
    w_s   = round(base * 1.015, 2)
    diff  = round(abs(med - ave), 2)
    cubic = round(2 + i * 0.35, 2)
    cost  = round(base * 0.95, 2)
    selling    = round(base * 3.2, 2)
    shopify_p  = round(base * 3.0, 2)
    kogan_au_p = round(base * 3.4, 2)
    kogan_k1_p = round(base * 3.5, 2)
    kogan_nz_p = round(base * 3.3, 2)
    # ★ 按公式计算 adjust：Selling Price < 25 才有值，否则 None
    adjust_val = round(selling * 0.04, 2) if selling < 25 else None

    return {
        "id": f"f-{i:03d}",
        "sku_code": sku,
        "shipping_type": st,
        "zone": zone,
        "adjust": adjust_val,
        "same_shipping": same,
        "shipping_ave": ave,
        "shipping_ave_m": ave_m,
        "shipping_ave_r": ave_r,
        "shipping_med": med,
        "remote_check": (i % 5 == 0),
        "rural_ave": rural,
        "weighted_ave_s": w_s,
        "shipping_med_dif": diff,
        "cubic_weight": cubic,
        "cost": cost,
        "selling_price": selling,
        "shopify_price": shopify_p,
        "kogan_au_price": kogan_au_p,
        "kogan_k1_price": kogan_k1_p,
        "kogan_nz_price": kogan_nz_p,
        "tags": [tag],
        "updated_at": _now,
    }

FREIGHT_RESULTS: List[dict] = [
    _mk(1,  "V201-001", "0",      "VIC_M", 10.0, "DropShippingZone"),
    _mk(2,  "V201-002", "1",      "VIC_R", 5.5, "Outdoor"),
    _mk(3,  "V201-003", "10",     "NSW_M", 14.0, "DropShippingZone"),
    _mk(4,  "V201-004", "15",     "NSW_R", 16.6, "Outdoor"),
    _mk(5,  "A100-001", "20",     "QLD_M", 18.1, "DropShippingZone"),
    _mk(6,  "A100-002", "Extra2", "QLD_R", 21.2, "Outdoor"),
    _mk(7,  "V202-001", "Extra3", "SA_M",  22.0, "DropShippingZone"),
    _mk(8,  "V202-002", "Extra4", "SA_R",  24.1, "Outdoor"),
    _mk(9,  "K900-111", "Extra5", "WA_M",  26.1, "DropShippingZone"),
    _mk(10, "K900-222", "10",     "WA_R",  28.2, "Outdoor"),
    _mk(11, "X500-010", "0",      "TAS_M", 11.0, "DropShippingZone"),
    _mk(12, "X500-011", "1",      "NT_M",  13.6, "Outdoor"),
    _mk(13, "Z777-001", "20",     "ACT_M", 19.3, "Own"),
    _mk(14, "Z777-002", "15",     "ACT_R", 17.4, "DropShippingZone"),
    _mk(15, "M321-999", "Extra3", "SA_M",  23.5, "Clearance"),
]