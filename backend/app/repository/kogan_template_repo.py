
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.model.freight import SkuFreightFee
from app.db.model.kogan_au_template import KoganTemplate
from app.db.model.kogan_export_job import (
    ExportJobStatus,
    KoganExportJob,
    KoganExportJobSku,
)



"""
分页迭代待导出的 运费结果表中本次更新/新增的运费结果：
    以批次形式迭代返回“需要导出的 SKU 列表”。
    - 当 only_dirty=True：WHERE kogan_dirty=true
    - 当提供 freight_run_id：WHERE last_changed_run_id=...
    - 两者都提供时，取交集条件（更严格）
    """
def iter_changed_skus(
    db: Session,
    batch_size: int = 5000,
) -> Iterator[List[str]]:
    
    # 分页迭代待导出的 SKU（固定：WHERE kogan_dirty=true）
    q = (
        db.query(SkuFreightFee.sku_code)
        .filter(SkuFreightFee.kogan_dirty.is_(True))
        .order_by(SkuFreightFee.sku_code.asc())
    )

    # 用 offset/limit 分页；4 万级别可接受。如需更大规模可改为 keyset 分页。
    # 默认一批 5000 todo 配置修改？
    offset = 0
    while True:
        batch = q.offset(offset).limit(batch_size).all()
        if not batch:
            break
        skus = [r.sku_code for r in batch]
        yield skus
        offset += batch_size



# 读取 KoganTemplate 表的历史基线，返回 {sku: ORM对象}，供 service 做列级 diff 使用
def load_kogan_baseline_map(db: Session, country_type: str, skus: List[str]) -> Dict[str, KoganTemplate]:

    if not skus:
        return {}

    rows: List[KoganTemplate] = (
        db.query(KoganTemplate)
        .filter(
            KoganTemplate.country_type == country_type,
            KoganTemplate.sku.in_(skus),
        )
        .all()
    )
    return {r.sku: r for r in rows}




'''
创建一条 KoganExportJob 记录及其关联的 KoganExportJobSku 记录
'''
def create_export_job(
    db: Session,
    *,
    country_type: str,
    file_name: str,
    file_bytes: bytes,
    row_count: int,
    created_by: Optional[int],
    sku_records: Sequence[dict],
) -> KoganExportJob:
    
    job = KoganExportJob(
        country_type=country_type,
        status=ExportJobStatus.EXPORTED,
        file_name=file_name,
        file_size=len(file_bytes),
        row_count=row_count,
        file_content=file_bytes,
        created_by=created_by,
        exported_at=datetime.now(timezone.utc),
    )

    db.add(job)
    db.flush()

    if sku_records:
        entries = [
            KoganExportJobSku(
                job_id=job.id,
                sku=rec["sku"],
                template_payload=rec["template_payload"],
                changed_columns=list(rec.get("changed_columns", [])),
            )
            for rec in sku_records
        ]
        db.add_all(entries)

    db.commit()
    db.refresh(job)
    return job



# 获取导出任务及其文件内容；找不到则抛错
def get_export_job(db: Session, job_id: uuid.UUID) -> Optional[KoganExportJob]:
    return (
        db.query(KoganExportJob)
        .options(selectinload(KoganExportJob.skus))
        .filter(KoganExportJob.id == job_id)
        .one_or_none()
    )



# 获取导出任务及其文件内容；找不到则抛错
def mark_job_status(
    db: Session,
    job: KoganExportJob,
    *,
    status: str,
    note: Optional[str] = None,
    applied_by: Optional[int] = None,
) -> None:
    job.status = status
    if status == ExportJobStatus.APPLIED:
        job.applied_at = datetime.now(timezone.utc)
        job.applied_by = applied_by
    if status == ExportJobStatus.EXPORTED:
        job.exported_at = datetime.now(timezone.utc)
    if note is not None:
        job.note = note
    db.add(job)
    db.commit()
    db.refresh(job)




# 把前端确认“导出成功”时的变更回写到 kogan_template 表，并把相关 SKU 的 kogan_dirty 置回 false
def apply_kogan_template_updates(
    db: Session,
    *,
    country_type: str,
    updates: Sequence[dict],
) -> None:
    if not updates:
        return

    skus = [item["sku"] for item in updates]
    existing = load_kogan_baseline_map(db, country_type, skus)

    for rec in updates:
        sku = rec["sku"]
        values = rec["values"]
        row = existing.get(sku)
        if row is None:
            row = KoganTemplate(sku=sku, country_type=country_type)
            db.add(row)
            existing[sku] = row
        for col, val in values.items():
            setattr(row, col, val)


def clear_kogan_dirty_flags(db: Session, skus: Sequence[str]) -> None:
    if not skus:
        return
    (
        db.query(SkuFreightFee)
        .filter(SkuFreightFee.sku_code.in_(skus))
        .update({SkuFreightFee.kogan_dirty: False}, synchronize_session=False)
    )
