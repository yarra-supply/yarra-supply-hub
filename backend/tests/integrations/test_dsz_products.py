"""Integration debug checks for DSZProductsAPI real requests."""

from __future__ import annotations

from typing import Iterable, List

import pytest

from app.core.config import settings
import json
from app.integrations.dsz.dsz_products import (
    DSZProductsAPI,
    get_products_by_skus,
    get_products_by_skus_with_stats,
)


def _has_dsz_credentials() -> bool:
    """Return True when DSZ credentials are configured for live debugging."""
    return bool(settings.DSZ_API_EMAIL and settings.DSZ_API_PASSWORD)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _has_dsz_credentials(),
        reason="DSZ credentials (DSZ_API_EMAIL / DSZ_API_PASSWORD) not configured.",
    ),
]


_DEFAULT_SAMPLE_SKUS: List[str] = [
    # "V420-CSST-WPOTGSET-C",
    # "V952-GYBGSFS18INCH2HS2V3",
    # "V922-AWD-D0532-PHOALB600-BR",
    # "BW-CLEANER-58771",
    # "EAC-C-RC-01L-BK",
    # "GL-ECO-3T-1000",
    # "V201-HAZ0000WH8AU",
    # "V240-CMT-JFA-180-LED",
    # "V201-HOLD6079WH8AU",
    # "V178-36220",
    "BAS-HOOP-160-RDBK"
]

def _sample_skus() -> List[str]:
    """Resolve the SKU list for integration debugging."""
    return list(_DEFAULT_SAMPLE_SKUS)


@pytest.fixture(scope="module")
def dsz_products_api() -> Iterable[DSZProductsAPI]:
    """Create one DSZProductsAPI per module to reuse across debug tests."""
    api = DSZProductsAPI()
    yield api


@pytest.fixture(scope="module")
def sample_skus() -> List[str]:
    """Provide a reusable list of SKUs for live DSZ calls."""
    skus = _sample_skus()
    if not skus:
        pytest.skip(
            "Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to run DSZProductsAPI integration tests."
        )
    return skus



# 基础冒烟，确认 fetch_by_skus 返回的确是非空 list[dict]
def test_fetch_by_skus_returns_product_list(
    dsz_products_api: DSZProductsAPI, sample_skus: List[str]
) -> None:
    """基础冒烟：确认 fetch_by_skus 能返回非空的 list[dict]。"""
    chunk = sample_skus[: min(len(sample_skus), 3)]
    if not chunk:
        pytest.skip("Need at least one SKU to exercise DSZProductsAPI.fetch_by_skus")

    print(f"[fetch_by_skus] Requesting {len(chunk)} SKUs: {chunk}")
    products = dsz_products_api.fetch_by_skus(chunk)

    assert isinstance(products, list), "Expected fetch_by_skus to return list[dict]"
    assert products, "Empty product list returned; confirm SKUs are valid in DSZ."
    assert all(isinstance(item, dict) for item in products), "Non-dict payload encountered"
    first = products[0] if products else {}
    print("[fetch_by_skus] Received", len(products), "products:")
    print(json.dumps(products, ensure_ascii=False, indent=2))


# 对统计字段做核对，确保 return_stats=True 时请求/响应数量匹配
def test_fetch_by_skus_with_stats_counts_match(
    dsz_products_api: DSZProductsAPI, sample_skus: List[str]
) -> None:
    """统计核对：验证 return_stats=True 时统计值与请求/响应数量一致。"""
    chunk = sample_skus[: min(len(sample_skus), 5)]
    if not chunk:
        pytest.skip("Need at least one SKU to verify stats payload")

    print(f"[fetch_with_stats] Requesting {len(chunk)} SKUs: {chunk}")
    products, stats = dsz_products_api.fetch_by_skus(chunk, return_stats=True)

    print("[fetch_with_stats] Received", len(products), "products:")
    print(json.dumps(products, ensure_ascii=False, indent=2))

    assert isinstance(stats, dict), "Stats payload should be a dict"
    assert stats["requested_total"] == len(chunk), "Stats requested_total mismatch"
    assert stats["returned_total"] == len(products), "Stats returned_total mismatch"
    assert stats["failed_batches_count"] == 0, f"Unexpected failed batch: {stats}"
    print(f"[fetch_with_stats] Stats summary: {stats}")



def test_module_helpers_execute_real_calls(sample_skus: List[str]) -> None:
    """模块封装：调用模块级 helper，确认能打真实接口并拿到统计。"""
    chunk = sample_skus[: min(len(sample_skus), 2)]
    if not chunk:
        pytest.skip("Need at least one SKU to test module helpers")

    print(f"[helpers] Requesting SKUs via get_products_by_skus: {chunk}")
    products_only = get_products_by_skus(chunk)
    assert isinstance(products_only, list)
    assert products_only, "Helper returned empty list; verify provided SKUs."

    products_with_stats, stats = get_products_by_skus_with_stats(chunk)
    assert isinstance(products_with_stats, list)
    assert isinstance(stats, dict)
    assert stats["requested_total"] == len(chunk)
    print(f"[helpers] Stats summary: {stats}")
