"""Integration-style test that steps through the Kogan template export pipeline.

可以在本地调试（step by step）真实的数据流：创建导出任务 → 下载 CSV →
重新下载 → 应用回写，流程与 /backend/tests/orchestration/test_freight_task.py 类似。
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from fastapi import Response
from sqlalchemy import text

from app.api.v1.kogan_template_download import (
    create_kogan_template_export,
    download_export_job,
    download_kogan_template_diff_csv,
    apply_kogan_export,
)
from app.db.model.freight import SkuFreightFee
from app.db.model.kogan_au_template import KoganTemplate
from app.db.model.kogan_export_job import KoganExportJob, KoganExportJobSku
from app.db.model.product import SkuInfo
from app.db.session import SessionLocal


FAKE_USER = {"username": "integration-debug"}


def _db_ready() -> bool:
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        return True
    except Exception:
        return False


def _cleanup(db, sku: str, job_id: str | None = None) -> None:
    db.query(KoganExportJobSku).filter(KoganExportJobSku.sku == sku).delete()
    if job_id is not None:
        db.query(KoganExportJob).filter(KoganExportJob.id == job_id).delete()
    db.query(KoganTemplate).filter(
        KoganTemplate.sku == sku,
        KoganTemplate.country_type == "AU",
    ).delete()
    db.query(SkuFreightFee).filter(SkuFreightFee.sku_code == sku).delete()
    db.query(SkuInfo).filter(SkuInfo.sku_code == sku).delete()
    db.commit()


def _prepare_seed_data(sku: str) -> None:
    db = SessionLocal()
    try:
        _cleanup(db, sku)

        product = SkuInfo(
            sku_code=sku,
            attrs_hash_current="debug-hash",
            stock_qty=3,
            price=Decimal("19.90"),
            rrp_price=Decimal("29.90"),
            brand="DebugBrand",
            weight=Decimal("1.200"),
            ean_code="1234567890123",
        )

        freight = SkuFreightFee(
            sku_code=sku,
            kogan_dirty=True,
            shipping_ave=Decimal("8.50"),
            weight=Decimal("1.250"),
            cubic_weight=Decimal("1.500"),
            kogan_au_price=Decimal("39.90"),
            kogan_k1_price=Decimal("35.90"),
            kogan_nz_price=Decimal("42.50"),
            shipping_type="extra3",
        )

        baseline = KoganTemplate(
            sku=sku,
            country_type="AU",
            price=Decimal("34.90"),
            shipping="0",
            handling_days=5,
            stock=10,
        )

        db.add_all([product, freight, baseline])
        db.commit()
    finally:
        db.close()


@pytest.mark.integration
def test_kogan_template_export_download_apply_flow():
    if not _db_ready():
        pytest.skip("Database connection is not available for Kogan template integration test")

    test_sku = f"DEBUG-KOGAN-{uuid.uuid4().hex[:6]}"
    _prepare_seed_data(test_sku)

    job_id: str | None = None

    try:
        # --- 1. 创建导出任务 ---
        with SessionLocal() as db:
            payload = create_kogan_template_export(
                country_type="AU",
                db=db,
                current_user=FAKE_USER,
            )
        job_id = payload["job_id"]
        assert payload["row_count"] == 1

        # --- 2. 下载 CSV ---
        with SessionLocal() as db:
            resp: Response = download_kogan_template_diff_csv(
                job_id=job_id,
                db=db,
                current_user=FAKE_USER,
            )
        assert resp.status_code == 200
        assert resp.headers.get("X-Kogan-Export-Job") == job_id
        assert resp.content.startswith(b"SKU"), "CSV should contain header row"

        # --- 3. 重新下载接口也可使用 ---
        with SessionLocal() as db:
            resp2: Response = download_export_job(
                job_id=job_id,
                db=db,
                current_user=FAKE_USER,
            )
        assert resp2.content == resp.content

        # --- 4. 应用导出结果 ---
        with SessionLocal() as db:
            apply_result = apply_kogan_export(
                job_id=job_id,
                db=db,
                current_user=FAKE_USER,
            )
        assert apply_result["status"] == "applied"

        # --- 5. 校验数据库回写情况 ---
        with SessionLocal() as db:
            template = (
                db.query(KoganTemplate)
                .filter(
                    KoganTemplate.sku == test_sku,
                    KoganTemplate.country_type == "AU",
                )
                .one()
            )
            assert template.price == Decimal("39.90")
            assert template.kogan_first_price == Decimal("35.90")
            assert template.shipping == "variable"
            assert template.handling_days is None
            assert template.stock == 3

            freight_row = db.get(SkuFreightFee, test_sku)
            assert freight_row is not None and freight_row.kogan_dirty is False

    finally:
        with SessionLocal() as db:
            _cleanup(db, test_sku, job_id)
