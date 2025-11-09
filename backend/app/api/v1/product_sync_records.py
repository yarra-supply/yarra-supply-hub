from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.repository.product_record_repo import (
    fetch_product_sync_runs_page,
    fetch_product_sync_chunks_page,
)
from app.db.model.product import ProductSyncRun, ProductSyncChunk
from app.services.auth_service import get_current_user


router = APIRouter(
    prefix="/product-sync-records",
    tags=["product-sync"],
    dependencies=[Depends(get_current_user)],
)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class ProductSyncRunOut(BaseModel):
    id: UUID
    run_type: Optional[str] = None
    status: str
    shopify_bulk_id: Optional[str] = None
    shopify_bulk_status: Optional[str] = None
    shopify_bulk_url: Optional[str] = None
    total_shopify_skus: Optional[int] = None
    changed_count: Optional[int] = None
    note: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    webhook_received_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class ProductSyncRunsPage(BaseModel):
    items: List[ProductSyncRunOut]
    total: int


class ProductSyncChunkOut(BaseModel):
    # id: int
    run_id: UUID
    chunk_idx: int
    status: str
    # sku_codes: List[str]
    sku_count: int
    dsz_missing: int
    # dsz_failed_batches: int
    dsz_failed_skus: int
    dsz_requested_total: int
    dsz_returned_total: int
    dsz_missing_sku_list: List[str]
    dsz_failed_sku_list: List[str]
    dsz_extra_sku_list: List[str]
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_error: Optional[str] = None
    # created_at: datetime
    # updated_at: datetime


class ProductSyncChunksPage(BaseModel):
    items: List[ProductSyncChunkOut]
    total: int


@router.get("/runs", response_model=ProductSyncRunsPage)
def list_product_sync_runs(
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: Optional[int] = Query(None, alias="page_size", ge=1, le=200, description="每页条数"),
    size: Optional[int] = Query(None, alias="size", ge=1, le=200, description="兼容旧参数名 size"),
    db: Session = Depends(get_db),
):
    ps = page_size or size or 20
    rows, total = fetch_product_sync_runs_page(db, page=page, page_size=ps)
    items = [_build_run_out(row) for row in rows]
    return ProductSyncRunsPage(items=items, total=total)




@router.get("/chunks", response_model=ProductSyncChunksPage)
def list_product_sync_chunks(
    run_id: Optional[UUID] = Query(None, description="按 run_id 过滤"),
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: Optional[int] = Query(None, alias="page_size", ge=1, le=200, description="每页条数"),
    size: Optional[int] = Query(None, alias="size", ge=1, le=200, description="兼容旧参数名 size"),
    db: Session = Depends(get_db),
):
    ps = page_size or size or 20
    rows, total = fetch_product_sync_chunks_page(
        db,
        page=page,
        page_size=ps,
        run_id=run_id,
    )
    items = [_build_chunk_out(row) for row in rows]
    return ProductSyncChunksPage(items=items, total=total)


def _build_run_out(row: ProductSyncRun) -> ProductSyncRunOut:
    return ProductSyncRunOut(
        id=row.id,
        run_type=row.run_type,
        status=row.status,
        shopify_bulk_id=row.shopify_bulk_id,
        shopify_bulk_status=row.shopify_bulk_status,
        shopify_bulk_url=row.shopify_bulk_url,
        total_shopify_skus=row.total_shopify_skus,
        changed_count=row.changed_count,
        note=row.note,
        started_at=row.started_at,
        finished_at=row.finished_at,
        webhook_received_at=row.webhook_received_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _build_chunk_out(row: ProductSyncChunk) -> ProductSyncChunkOut:
    return ProductSyncChunkOut(
        id=row.id,
        run_id=row.run_id,
        chunk_idx=row.chunk_idx,
        status=row.status,
        sku_codes=row.sku_codes or [],
        sku_count=row.sku_count,
        dsz_missing=row.dsz_missing,
        dsz_failed_batches=row.dsz_failed_batches,
        dsz_failed_skus=row.dsz_failed_skus,
        dsz_requested_total=row.dsz_requested_total,
        dsz_returned_total=row.dsz_returned_total,
        dsz_missing_sku_list=row.dsz_missing_sku_list or [],
        dsz_failed_sku_list=row.dsz_failed_sku_list or [],
        dsz_extra_sku_list=row.dsz_extra_sku_list or [],
        started_at=row.started_at,
        finished_at=row.finished_at,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
