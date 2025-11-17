from __future__ import annotations

from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.db.model.product import ProductSyncRun, ProductSyncChunk


def fetch_product_sync_runs_page(
    db: Session,
    *,
    page: int,
    page_size: int,
) -> Tuple[List[ProductSyncRun], int]:
    """分页查询 product_sync_runs，按创建时间倒序返回"""

    offset = (page - 1) * page_size

    total_stmt = select(func.count()).select_from(ProductSyncRun)
    total = db.scalar(total_stmt) or 0

    rows_stmt = (
        select(ProductSyncRun)
        .order_by(ProductSyncRun.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    rows = list(db.scalars(rows_stmt))

    return rows, total


def fetch_product_sync_chunks_page(
    db: Session,
    *,
    page: int,
    page_size: int,
    run_id: Optional[UUID] = None,
) -> Tuple[List[ProductSyncChunk], int]:
    """分页查询 product_sync_chunks，可按 run_id 过滤"""

    offset = (page - 1) * page_size

    base_stmt = select(ProductSyncChunk)
    count_stmt = select(func.count()).select_from(ProductSyncChunk)

    if run_id:
        base_stmt = base_stmt.where(ProductSyncChunk.run_id == run_id)
        count_stmt = count_stmt.where(ProductSyncChunk.run_id == run_id)

    total = db.scalar(count_stmt) or 0

    rows_stmt = (
        base_stmt
        .order_by(
            ProductSyncChunk.created_at.desc(),
            ProductSyncChunk.chunk_idx,
        )
        .offset(offset)
        .limit(page_size)
    )
    rows = list(db.scalars(rows_stmt))

    return rows, total
