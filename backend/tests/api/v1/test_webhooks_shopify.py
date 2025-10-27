"""最小限度地调用 bulk_finish，便于在真实依赖下调试流程。"""

import json

import pytest
from starlette.requests import Request

from app.api.v1.webhooks_shopify import bulk_finish, _compute_hmac_base64
from app.core.config import settings


def _build_request(raw_body: bytes, headers: dict[str, str]) -> Request:
    """根据原始请求体和 Header 构造 Starlette Request 对象。"""

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/webhooks/shopify/bulk_operations/finish",
        "query_string": b"",
        "headers": [(k.lower().encode("utf-8"), v.encode("utf-8")) for k, v in headers.items()],
        "client": ("127.0.0.1", 0),
        "server": ("testserver", 80),
        "scheme": "https",
    }

    state = {"sent": False}

    async def receive():
        if state["sent"]:
            return {"type": "http.disconnect"}
        state["sent"] = True
        return {"type": "http.request", "body": raw_body, "more_body": False}

    return Request(scope, receive)


@pytest.mark.asyncio
async def test_manual_bulk_finish_real_flow(monkeypatch):
    """直接调用 bulk_finish，不 mock 内部依赖，方便断点调试。"""

    payload = {
        "admin_graphql_api_id": "gid://shopify/BulkOperation/1234567890",
        "status": "completed",
        "object_count": 3,
        "url": "https://example.com/result.ndjson",
    }
    raw_body = json.dumps(payload).encode("utf-8")

    secret = "dev-secret"  # 本地调试使用的临时值，可按需修改
    monkeypatch.setattr(settings, "SHOPIFY_WEBHOOK_SECRET", secret)

    hmac_header = _compute_hmac_base64(secret, raw_body)

    headers = {
        "x-shopify-topic": "bulk_operations/finish",
        "x-shopify-hmac-sha256": hmac_header,
        "x-shopify-shop-domain": "example.myshopify.com",
    }

    request = _build_request(raw_body, headers)

    response = await bulk_finish(
        request,
        x_shopify_hmac_sha256=headers["x-shopify-hmac-sha256"],
        x_shopify_topic=headers["x-shopify-topic"],
        x_shopify_shop_domain=headers["x-shopify-shop-domain"],
    )

    print("[debug] bulk_finish 返回", response)
