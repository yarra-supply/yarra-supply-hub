"""Integration checks for Shopify client behaviour used during manual debugging."""

import json
import os
import time

import pytest

from app.core.config import settings
from app.integrations.shopify.shopify_client import ShopifyClient


_DEFAULT_TAG = (
    getattr(settings, "SHOPIFY_TAG_FULL_SYNC", None)
    or "DropshipzoneAU"
)



def _has_shopify_credentials() -> bool:
    token = settings.SHOPIFY_ADMIN_TOKEN
    if hasattr(token, "get_secret_value"):
        token = token.get_secret_value()
    return bool(settings.SHOPIFY_SHOP and token)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _has_shopify_credentials(),
        reason="Shopify credentials are not configured (SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN).",
    ),
]


@pytest.fixture(scope="function")
def shopify_client() -> ShopifyClient:
    return ShopifyClient()


@pytest.fixture(scope="module")
def shopify_tag() -> str:
    return _DEFAULT_TAG


# 方法 1：验证 ping() 能拿到基础店铺信息，说明 token/domain 生效。
def test_shopify_ping_returns_shop_metadata(shopify_client: ShopifyClient):
    resp = shopify_client.ping()
    shop = (resp.get("data") or {}).get("shop") or {}
    print("[ping response]", json.dumps(resp, ensure_ascii=False))
    assert shop.get("name"), f"Unexpected ping payload: {resp}"
    assert shop.get("myshopifyDomain"), f"Unexpected ping payload: {resp}"


# 方法 2：验证普通 GraphQL 查询（非 bulk）能查出商品。
def test_products_by_tag_query_returns_edges(shopify_client: ShopifyClient, shopify_tag: str):
    products = shopify_client.query_products_by_tag(
        shopify_tag,
        first=10,
        variants_first=10,
    )

    edges = products.get("edges")
    assert edges is not None, "products connection missing edges"
    if edges:
        print("[products sample count]", len(edges))
        for idx, edge in enumerate(edges[:10]):
            node = edge.get("node") or {}
            print(f"[products sample {idx}]", json.dumps(node, ensure_ascii=False))
        assert node.get("id"), "product node missing id"
        variants = node.get("variants") or {}
        variant_edges = variants.get("edges") or []
        if variant_edges:
            first_variant = variant_edges[0].get("node") or {}
            assert first_variant.get("id"), "variant node missing id"


# 方法 3：触发小规模 bulk（10 个商品），等待执行完成并输出摘要。
def test_run_small_bulk_operation_and_waits_for_result(
    shopify_client: ShopifyClient,
    shopify_tag: str,
):
    
    # 1、 查询current bulk operation
    current = shopify_client.current_bulk_operation() or {}
    query_text = current.get("query") or ""

    if current.get("status") in {"CREATED", "RUNNING"} and shopify_tag in query_text:
        bulk_id = current.get("id")
        print(f"[bulk reuse] Existing bulk is {current.get('status')} with id={bulk_id}")
        started = current
        detail = _wait_until_completed(shopify_client, bulk_id)
        current_snapshot = current
    else:
        # 测试全量
        # started = shopify_client.run_bulk_products_by_tag(
        #     shopify_tag,
        #     products_first=10,
        #     variants_first=50,
        # )

        # 测试前10
        started = shopify_client.run_bulk_products_by_tag(
            tag="DropshipzoneAU",
            variants_first=50,
            limit_override=10,     # [CHANGED] 只生成 first:10 的“单页版”内层查询
        )
        bulk_id = started.get("id")
        assert bulk_id, f"Bulk operation not started: {started}"

        current_snapshot = shopify_client.current_bulk_operation() or {}
        if not current_snapshot.get("query"):
            time.sleep(2)
            current_snapshot = shopify_client.current_bulk_operation() or current_snapshot

        detail = _wait_until_completed(shopify_client, bulk_id)
    summary = {
        "bulk_id": bulk_id,
        "status": detail.get("status"),
        "object_count": detail.get("objectCount"),
        "url": detail.get("url"),
        "query": current_snapshot.get("query"),
    }
    print(f"[bulk summary] {summary}")  # 便于 debug 查看中间结果

    assert summary["status"] == "COMPLETED"
    assert summary["object_count"] is not None
    assert summary["url"], "Bulk completed but no download URL returned"
    assert shopify_tag in (summary["query"] or ""), "Unexpected bulk query filter"




# 方法 4：根据手动填入的 bulk 结果 URL 下载 JSONL，检查数据格式。
def test_download_bulk_payload_from_manual_url(shopify_client: ShopifyClient):

    # todo 等待方法3的结果
    url = ("https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/bulk-operation-outputs/8mbf3ns5ub56bt83xglqky6pqbp7-final?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1761991688&Signature=G%2Bp5Gp638X%2BcRonKMct1XxsI5cuEmONrN%2BxXToeBkEYtFqdmsYz30dapm0srv%2FbwwbYUCl2Te2xyaoCMepBhrcu8jslYzHI9CBf8OZPmRyY6A5GBjcGU3WJagae%2FxE3Ldjsam1ka6b1EM1IN0jcpb%2BX6n3UvnceszYWUiNokdsvVQMM1mEDh52xRoo5umq3HTueshG2ePhbLTyigCg7mVmxuRNto1giDIZdT14vQqfn%2FVcC8kkwgqICHCxIRzvM1b142AChB72vxjG1b1BVl4YK4jkj%2Bmmp6dEwekgPPs9MGZK1qOJdV21pZDTF0RyJoDfwKg6R65v3EWgIuTJpyNw%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-5296287809618.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-5296287809618.jsonl&response-content-type=application%2Fjsonl" or "").strip()
    if not url:
        pytest.skip(
            "Manual bulk download URL is missing. Set SHOPIFY_BULK_MANUAL_URL or edit _MANUAL_BULK_DOWNLOAD_URL."
        )

    lines = []
    for idx, line in enumerate(shopify_client.download_jsonl_stream(url)):
        lines.append(line)
        print("[bulk jsonl line]", line)
        if idx >= 4:  # 只取前几行验证格式，避免一次读完全部。
            break
    
    # 统计objectCount
    total_count = sum(1 for _ in shopify_client.download_jsonl_stream(url))
    print("[bulk jsonl total count]", total_count)

    # 追加：按“根对象 / 变体 / 其他”做粗略统计并打印，rootObjectCount
    # 为了不改全局 import，这里局部导入
    import urllib.request, collections  # [CHANGED]
    root = 0                            # [CHANGED]
    variants = 0                        # [CHANGED]
    others = collections.Counter()      # [CHANGED]
    with urllib.request.urlopen(url) as r:                 # [CHANGED]
        for b in r:                                        # [CHANGED]
            s = b.decode("utf-8").strip()                  # [CHANGED]
            if not s:                                      # [CHANGED]
                continue                                   # [CHANGED]
            o = json.loads(s)                              # [CHANGED]
            # 根对象（产品）：通常没有 __parentId                    # [CHANGED]
            if "__parentId" not in o:                      # [CHANGED]
                root += 1                                  # [CHANGED]
            # 变体：包含 sku（你的查询里也确实取了 variant.sku）       # [CHANGED]
            elif "sku" in o:                               # [CHANGED]
                variants += 1                              # [CHANGED]
            else:                                          # [CHANGED]
                key = o.get("__typename") or "other"       # [CHANGED]
                others[key] += 1                           # [CHANGED]
    print("[bulk breakdown] root(products)=", root,         # [CHANGED]
          "variants=", variants, "others=", dict(others))   # [CHANGED]

    assert lines, "No JSONL data returned from bulk download"
    for raw in lines:
        json.loads(raw)



# 手动输入 bulk id 和 url，再根据 id 查询运行情况
def test_inspect_existing_bulk_operation(shopify_client: ShopifyClient):
    bulk_id = "gid://shopify/BulkOperation/5296287809618"
    url_hint = "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/bulk-operation-outputs/8mbf3ns5ub56bt83xglqky6pqbp7-final?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1761991688&Signature=G%2Bp5Gp638X%2BcRonKMct1XxsI5cuEmONrN%2BxXToeBkEYtFqdmsYz30dapm0srv%2FbwwbYUCl2Te2xyaoCMepBhrcu8jslYzHI9CBf8OZPmRyY6A5GBjcGU3WJagae%2FxE3Ldjsam1ka6b1EM1IN0jcpb%2BX6n3UvnceszYWUiNokdsvVQMM1mEDh52xRoo5umq3HTueshG2ePhbLTyigCg7mVmxuRNto1giDIZdT14vQqfn%2FVcC8kkwgqICHCxIRzvM1b142AChB72vxjG1b1BVl4YK4jkj%2Bmmp6dEwekgPPs9MGZK1qOJdV21pZDTF0RyJoDfwKg6R65v3EWgIuTJpyNw%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-5296287809618.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-5296287809618.jsonl&response-content-type=application%2Fjsonl"

    current_snapshot = shopify_client.current_bulk_operation() or {}
    detail = _wait_until_completed(shopify_client, bulk_id)
    summary = {
        "bulk_id": bulk_id,
        "status": detail.get("status"),
        "object_count": detail.get("objectCount"),
        "url": detail.get("url") or url_hint,
        "query": current_snapshot.get("query"),
    }
    print(f"[bulk summary inspect] {summary}")



def _wait_until_completed(client: ShopifyClient, bulk_id: str, timeout_s: int = 600) -> dict:
    poll = max(5, int(getattr(settings, "SHOPIFY_BULK_POLL_INTERVAL_SEC", 8)))
    deadline = time.time() + timeout_s
    detail: dict = {}
    while time.time() < deadline:
        detail = client.get_bulk_operation_by_id(bulk_id) or {}
        status = detail.get("status")
        if status == "COMPLETED":
            return detail
        if status in {"FAILED", "CANCELED"}:
            pytest.fail(f"Bulk operation terminated early: {detail}")
        time.sleep(poll)
    pytest.fail(f"Bulk operation {bulk_id} not completed within {timeout_s}s")
