
from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.services.kogan_template_service import stream_kogan_diff_csv
from app.services.auth_service import get_current_user


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
):
    csv_iter, filename = stream_kogan_diff_csv(
        db=db,
        country_type=country_type,
    )

    return StreamingResponse(
        csv_iter,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )
