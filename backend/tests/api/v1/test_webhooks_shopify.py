# """最小限度地调用 bulk_finish，便于在真实依赖下调试流程。"""

# import asyncio
# import json

# import pytest
# from starlette.requests import Request

# from app.api.v1.webhooks_shopify import bulk_finish, _compute_hmac_base64
# from app.core.config import settings


# def _build_request(raw_body: bytes, headers: dict[str, str]) -> Request:
#     """根据原始请求体和 Header 构造 Starlette Request 对象。"""

#     scope = {
#         "type": "http",
#         "method": "POST",
#         "path": "/webhooks/shopify/bulk_operations/finish",
#         "query_string": b"",
#         "headers": [(k.lower().encode("utf-8"), v.encode("utf-8")) for k, v in headers.items()],
#         "client": ("127.0.0.1", 0),
#         "server": ("testserver", 80),
#         "scheme": "https",
#     }

#     state = {"sent": False}

#     async def receive():
#         if state["sent"]:
#             return {"type": "http.disconnect"}
#         state["sent"] = True
#         return {"type": "http.request", "body": raw_body, "more_body": False}

#     return Request(scope, receive)


# @pytest.mark.integration
# def test_bulk_finish_real_flow():
#     """使用真实 Shopify BulkOperation GID，走完整调用链进行验证。"""

#     bulk_gid = "gid://shopify/BulkOperation/5307836956754"
#     payload = {"admin_graphql_api_id": bulk_gid}
#     raw_body = json.dumps(payload).encode("utf-8")

#     secret = settings.SHOPIFY_WEBHOOK_SECRET
#     if not secret:
#         pytest.skip("SHOPIFY_WEBHOOK_SECRET 未配置")
#     if hasattr(secret, "get_secret_value"):
#         secret = secret.get_secret_value()

#     hmac_header = _compute_hmac_base64(secret, raw_body)

#     shop_domain = getattr(settings, "SHOPIFY_SHOP", None)
#     if not shop_domain:
#         pytest.skip("SHOPIFY_SHOP 未配置")

#     headers = {
#         "x-shopify-topic": "bulk_operations/finish",
#         "x-shopify-hmac-sha256": hmac_header,
#         "x-shopify-shop-domain": shop_domain,
#     }

#     async def _invoke():
#         request = _build_request(raw_body, headers)
#         return await bulk_finish(
#             request,
#             x_shopify_hmac_sha256=headers["x-shopify-hmac-sha256"],
#             x_shopify_topic=headers["x-shopify-topic"],
#             x_shopify_shop_domain=headers["x-shopify-shop-domain"],
#         )

#     response = asyncio.run(_invoke())

#     assert response["ok"] is True
#     assert "status" in response
#     assert "objectCount" in response
