
from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.services.kogan_template_service import (
    ExportJobNotFoundError,
    NoDirtySkuError,
    apply_export_job,
    create_kogan_export_job,
    get_export_job_file,
    serialize_export_job,
)
from app.services.auth_service import get_current_user


logger = logging.getLogger(__name__)


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



def _format_melbourne(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")



"""创建导出任务，返回 job 元数据（不返回文件）。"""
@router.post("/kogan-template/export")
def create_kogan_template_export(
    country_type: str = Query(..., regex="^(AU|NZ)$", description="AU or NZ"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        job = create_kogan_export_job(
            db=db,
            country_type=country_type,
            # created_by=(current_user or {}).get("id"),
            created_by=None,
        )
    except NoDirtySkuError as exc:
        return {
            "detail": "no_dirty_sku",
            "row_count": 0,
            "last_job": serialize_export_job(exc.last_job),
        }
    except Exception as exc:
        logger.exception("create_kogan_template_export failed for %s", country_type)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return serialize_export_job(job)



"""根据 job_id 下载已生成的 CSV。"""
@router.get("/kogan-template/download")
def download_kogan_template_diff_csv(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        job = get_export_job_file(db, job_id)
    except ExportJobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    file_bytes = bytes(job.file_content or b"")

    headers = {
        "Content-Disposition": f'attachment; filename="{quote(job.file_name)}"',
        "Cache-Control": "no-store",
        "Access-Control-Expose-Headers": "Content-Disposition, X-Kogan-Export-Job, X-Kogan-Export-Rows, X-Kogan-Export-Status, X-Kogan-Export-Applied-At, X-Kogan-Export-Exported-At, X-Kogan-Export-Country",
        "X-Kogan-Export-Job": str(job.id),
        "X-Kogan-Export-Rows": str(job.row_count),
        "X-Kogan-Export-Status": job.status,
        "X-Kogan-Export-Applied-At": _format_melbourne(job.applied_at),
        "X-Kogan-Export-Exported-At": _format_melbourne(job.exported_at),
        "X-Kogan-Export-Country": job.country_type,
        "Content-Length": str(len(file_bytes)),
    }

    return Response(content=file_bytes, media_type="text/csv; charset=utf-8", headers=headers)




'''
如果前端想重新下载刚才那份 CSV（比如用户刷新页面或需要再次获取同一文件），
就用上一步拿到的 job_id 调这个接口。后端直接从 kogan_export_jobs 里取之前保存的文件内容返回，不会重新计算
'''
@router.get("/kogan-template/export/{job_id}/download")
def download_export_job(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        job = get_export_job_file(db, job_id)
    except ExportJobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    
    file_bytes = bytes(job.file_content or b"")

    headers = {
        "Content-Disposition": f'attachment; filename="{quote(job.file_name)}"',
        "Cache-Control": "no-store",
        "Access-Control-Expose-Headers": "Content-Disposition, X-Kogan-Export-Job, X-Kogan-Export-Rows, X-Kogan-Export-Status, X-Kogan-Export-Applied-At, X-Kogan-Export-Exported-At, X-Kogan-Export-Country",
        "X-Kogan-Export-Job": str(job.id),
        "X-Kogan-Export-Rows": str(job.row_count),
        "X-Kogan-Export-Status": job.status,
        "X-Kogan-Export-Applied-At": _format_melbourne(job.applied_at),
        "X-Kogan-Export-Exported-At": _format_melbourne(job.exported_at),
        "X-Kogan-Export-Country": job.country_type,
        "Content-Length": str(len(file_bytes)),
    }
    
    return Response(content=file_bytes, media_type="text/csv; charset=utf-8", headers=headers)



''' 
当前端确认“导出成功”时调用，把那次 job 对应的变更回写到 kogan_template 表，
并把相关 SKU 的国家级脏标记置回 false，避免重复导出
'''
@router.post("/kogan-template/export/{job_id}/apply")
def apply_kogan_export(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        job = apply_export_job(
            db=db,
            job_id=job_id,
            # applied_by=(current_user or {}).get("id"),
            applied_by=None,
        )
    except ExportJobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    applied_at = job.applied_at.isoformat() if job.applied_at else None
    return {
        "job_id": str(job.id),
        "status": job.status,
        "applied_at": applied_at,
    }
