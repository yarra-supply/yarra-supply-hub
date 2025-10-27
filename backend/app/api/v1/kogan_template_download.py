
from __future__ import annotations
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException, Response
from fastapi.responses import StreamingResponse

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.services.kogan_template_service import stream_kogan_diff_csv
from app.services.auth_service import get_current_user

# mock 使用 后续delete
import csv, io
from datetime import datetime, timezone


router = APIRouter(
    tags=["kogan-template"],
    dependencies=[Depends(get_current_user)], 
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



"""
生成并下载“仅包含变化字段”的 Kogan 模版 CSV（流式输出）。
    - 如果提供 freight_run_id，则按该 run 的变化导出；
    - 否则默认按 kogan_dirty=true 导出；
    - 仅导出发生变化的行；行内仅填变化的列，其余列留空。
 """
# todo 测试分批流式输出
@router.get("/kogan-template/download")
def download_kogan_template_diff_csv(
    country_type: str = Query(..., regex="^(AU|NZ)$", description="AU or NZ"),
    db: Session = Depends(get_db),
    response: Response = None,
):
    
    # 实际流程
    csv_iter, filename = stream_kogan_diff_csv(
        db=db,
        country_type=country_type,
    )

    # mock输出
    headers = HEADERS_BY_COUNTRY[country_type]
    mock_rows = ROWS_BY_COUNTRY[country_type]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    for r in mock_rows:
        writer.writerow([r.get(h, "") for h in headers])

    # === 返回流（带文件名） ===
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # todo 现在输出2列output都有
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="kogan_upload_{country_type}_{ts}.csv"',
            "Cache-Control": "no-store",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )
    

    # 流怎么用？为什么用？接真实流再换 StreamingResponse + 迭代器
    # return StreamingResponse(
    #     csv_iter,
    #     media_type="text/csv; charset=utf-8",
    #     headers={
    #         "Content-Disposition": f'attachment; filename="{filename}"',
    #         "Cache-Control": "no-store",
    #         "Access-Control-Expose-Headers": "Content-Disposition",
    #     },
    # )



HEADERS_BY_COUNTRY = {
    "AU": [
        "SKU",
        "Price",
        "RRP",
        "Kogan First Price",
        "Handling Days",
        "Barcode",
        "Stock",
        "Shipping",
        "Weight",
        "Brand",
        "Title",
        "Description",
        "Subtitle",
        "What's in the Box",
        "SKU",
        "Category",
    ],
    "NZ": [
        "SKU",
        "Price",
        "RRP",
        "Kogan First Price",
        "Shipping",
        "Handling Days",
    ],
}

# === # Mock使用的 后续delete 稀疏 mock 数据：只有变化列写值 ===
ROWS_BY_COUNTRY = {
    "AU": [
        {"SKU": "A1001", "Price": "19.99"},  # 改价
        {"SKU": "A1002", "Shipping": "12.50"},  # 改运费
        {"SKU": "A1003", "Title": "New Title for A1003"},  # 改标题
        {"SKU": "A1004", "Stock": "0"},  # 改库存
        {"SKU": "A1005", "RRP": "39.99", "Price": "29.99"},  # 同时改 RRP/Price
        {"SKU": "A1006", "Barcode": "9345678901234"},  # 改条码
        {"SKU": "A1007", "Weight": "1.250"},  # 改重量
        {"SKU": "A1008", "Category": "Home > Kitchen"},  # 改分类
        {"SKU": "A1009", "Description": "Updated description text."},  # 改描述
        {"SKU": "A1010", "What's in the Box": "Cable, Charger", "Handling Days": "3"},  # 改盒内物品 + 处理天数
    ],
    "NZ": [
        {"SKU": "NZ2001", "Price": "24.99"},  # 改价
        {"SKU": "NZ2002", "Shipping": "18.00"},  # 改运费
        {"SKU": "NZ2003", "RRP": "59.99", "Price": "49.99"},  # 调整 RRP 与 Price
        {"SKU": "NZ2004", "Kogan First Price": "44.99"},  # 更新 Kogan First Price
        {"SKU": "NZ2005", "Handling Days": "5"},  # 调整处理天数
    ],
}
