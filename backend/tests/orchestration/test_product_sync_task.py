
import os
import time

import pytest

from app.orchestration.product_sync import product_sync_task
from app.integrations.shopify.shopify_client import ShopifyClient
from app.core.config import settings
from app.db.session import SessionLocal
from app.db.model.product import ProductSyncRun


@pytest.mark.integration
def test_sync_start_full_inline_integration(monkeypatch):
    """
    手动集成测试：需要真实的 Shopify/DSZ 环境与数据库。
    未配置 RUN_LIVE_PRODUCT_SYNC_TEST 时自动跳过，避免 CI 卡住。
    """
    # if not os.getenv("RUN_LIVE_PRODUCT_SYNC_TEST"):
    #     pytest.skip("set RUN_LIVE_PRODUCT_SYNC_TEST=1 to enable live product sync test")

    monkeypatch.setattr(product_sync_task.settings, "SYNC_TASKS_INLINE", True, raising=False)
    monkeypatch.setattr(product_sync_task.settings, "SYNC_BULK_PREVIEW_LIMIT", 20, raising=False)
    monkeypatch.setattr(product_sync_task.settings, "SHOPIFY_BULK_TEST_MODE", True, raising=False)

    run_id = product_sync_task.sync_start_full_inline()
    assert run_id


def _wait_for_bulk_completion(client: ShopifyClient, bulk_id: str, *, timeout_s: int = 600, interval_s: int = 5) -> dict:
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        last = client.get_bulk_operation_by_id(bulk_id) or {}
        status = last.get("status")
        if status in {"COMPLETED", "FAILED", "CANCELED"}:
            return last
        time.sleep(interval_s)
    raise TimeoutError(f"Bulk operation {bulk_id} did not finish within {timeout_s}s; last state={last}")


@pytest.mark.integration
def test_handle_bulk_finish_inline_integration(monkeypatch):
    # if not os.getenv("RUN_LIVE_PRODUCT_SYNC_TEST"):
    #     pytest.skip("set RUN_LIVE_PRODUCT_SYNC_TEST=1 to enable live product sync test")

    # monkeypatch.setattr(product_sync_task.settings, "SYNC_TASKS_INLINE", True, raising=False)
    # monkeypatch.setattr(product_sync_task.settings, "SYNC_BULK_PREVIEW_LIMIT", 20, raising=False)

    # client = ShopifyClient()
    # tag = getattr(settings, "SHOPIFY_TAG_FULL_SYNC", "DropshipzoneAU")

    # current = client.current_bulk_operation() or {}
    # if current.get("status") in {"CREATED", "RUNNING"}:
    #     query_text = current.get("query") or ""
    #     if tag not in query_text:
    #         pytest.skip(f"Another bulk operation is running: {current}")
    #     bulk_info = current
    # else:
    #     bulk_info = client.run_bulk_products_by_tag(tag, products_first=20, variants_first=20)

    # bulk_id = bulk_info.get("id")
    # assert bulk_id, f"Bulk operation not started: {bulk_info}"

    # detail = _wait_for_bulk_completion(client, bulk_id, timeout_s=900, interval_s=10)
    # if detail.get("status") != "COMPLETED" or not detail.get("url"):
    #     pytest.fail(f"Bulk operation did not succeed: {detail}")

    bulk_url = "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/bulk-operation-outputs/errpjg2jhp3mf0kqxnfhqpqpervq-final?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1762944882&Signature=a4SDxJdyYabH0udF0rCcR9UwdW6cRyedQPkEaFXQwK8NuCZNbCrS90QvaNTKzziE%2FiN3iA7VytrOQAMQrD1gNvBDY1rexV%2BQA8ssR0%2FODq0%2BnhMq6AH77tKMo6Akvhl4tPd1rQVteLqQYx9wNZoDdWlKFmsjRe4tplQW4laUT19ClC8zWUJePK8bruswek1vYkbh%2B%2FRalBUfLfWkTN23ujFRNiItTBLrFlRZZQdm97msEiPDpAfZvaAx5wPiPo9f0MSvNd2jTlienXD6AuRe8NrF6Ui5EspSKI%2FCSQnTRLnho8nKibl%2B2C8chksHmE%2BX9KoPgo3ndf4yUqOXcMWnRQ%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-5339960672338.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-5339960672338.jsonl&response-content-type=application%2Fjsonl"
    
    result = product_sync_task.handle_bulk_finish_inline(
        "gid://shopify/BulkOperation/5339960672338",
        bulk_url,
        49855,
    )

    run_id = (result or {}).get("run_id")
    assert run_id, f"handle_bulk_finish_inline did not return run_id: {result}"

    db = SessionLocal()
    try:
        run = db.get(ProductSyncRun, run_id)
        assert run is not None, "ProductSyncRun record missing"
        assert run.shopify_bulk_url == bulk_url
        assert run.shopify_bulk_status == "COMPLETED"
    finally:
        db.close()
