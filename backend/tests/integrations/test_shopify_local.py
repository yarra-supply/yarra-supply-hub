# # tests/test_shopify_integration.py
# """
# Shopify Admin GraphQL 集成测试（结合你的 config.py）
# 功能：
# 1) 基础店铺信息查询
# 2) 连通性测试（最小查询）
# 3) 按标签小查（少量商品）
# 4) 发起 BulkOperation（按标签）
# 5) 轮询 Bulk 直到完成并获取 url
# 6) 通过 BulkOperation 的 GID 查询详情（node(id: ...））

# VSCode 可单测/单步 Debug；每个 test_* 方法互相独立，只有 Bulk 的上下文通过 CTX 传递。
# """

# from __future__ import annotations
# import json
# import os
# import time
# from dataclasses import dataclass
# from typing import Any, Dict, Optional

# import pytest
# import requests

# from app.core.config import settings


# # ====== CHANGE: 根据你的 config 字段构造请求基础信息 ======
# SHOP_DOMAIN = settings.SHOPIFY_SHOP  # 例：yarra-supply.myshopify.com
# API_VERSION = settings.SHOPIFY_API_VERSION  # 例：2025-07（默认可在 .env 覆盖）
# ADMIN_TOKEN = (settings.SHOPIFY_ADMIN_TOKEN.get_secret_value()
#                if getattr(settings, "SHOPIFY_ADMIN_TOKEN", None)
#                else None)

# # 构造 Admin GraphQL 端点（注意你的 SHOP 字段已包含完整域名）
# BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json" if SHOP_DOMAIN else None

# # HTTP 参数来自你的 config
# HTTP_TIMEOUT = getattr(settings, "SHOPIFY_HTTP_TIMEOUT", 30)  # CHANGE
# BULK_POLL_INTERVAL = getattr(settings, "BULK_POLL_INTERVAL_SEC", 8)  # CHANGE
# TEST_TAG = getattr(settings, "SHOPIFY_TAG_FULL_SYNC", "DropshipzoneAU")  # CHANGE

# HEADERS = {
#     "X-Shopify-Access-Token": ADMIN_TOKEN or "",
#     "Content-Type": "application/json",
# }

# # 简单断言前置：没有 token 就跳过整组测试
# pytestmark = pytest.mark.skipif(
#     not ADMIN_TOKEN or not SHOP_DOMAIN,
#     reason="缺少 SHOPIFY_ADMIN_TOKEN 或 SHOPIFY_SHOP，请在 .env 中配置后再运行"
# )

# # 为了在多个测试用例之间传递 bulk_id / url 等信息
# @dataclass
# class BulkContext:
#     bulk_id: Optional[str] = None
#     url: Optional[str] = None
#     partial_url: Optional[str] = None
#     status: Optional[str] = None

# CTX = BulkContext()


# # -----------------------------
# # 小工具：GraphQL 请求封装
# # -----------------------------
# def shopify_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
#     assert BASE_URL, "BASE_URL 未构建（请检查 SHOPIFY_SHOP / SHOPIFY_API_VERSION）"
#     assert ADMIN_TOKEN, "ENV/配置缺少 SHOPIFY_ADMIN_TOKEN"

#     payload = {"query": query}
#     if variables:
#         payload["variables"] = variables

#     # CHANGE: 使用你配置中的 HTTP 超时
#     resp = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=HTTP_TIMEOUT)
#     try:
#         data = resp.json()
#     except Exception:
#         raise AssertionError(f"非 JSON 响应，HTTP {resp.status_code}, text={resp.text[:300]}")

#     # 便于 Debug：失败时打印
#     if resp.status_code != 200 or "errors" in data:
#         pretty = json.dumps(data, indent=2, ensure_ascii=False)
#         raise AssertionError(f"GraphQL 请求失败: HTTP={resp.status_code}\n{pretty}")

#     return data


# def current_bulk() -> Dict[str, Any]:
#     q = """
#     query {
#       currentBulkOperation(type: QUERY) {
#         id
#         status
#         createdAt
#         completedAt
#         objectCount
#         url
#         partialDataUrl
#       }
#     }
#     """
#     return shopify_graphql(q)


# # def cancel_bulk_if_running() -> Optional[str]:
# #     """若有运行中的 bulk，则尝试取消，返回被取消的 bulk id。"""
# #     data = current_bulk()
# #     cur = data.get("data", {}).get("currentBulkOperation")
# #     if not cur:
# #         return None
# #     status = cur.get("status")
# #     bid = cur.get("id")
# #     if status in ("CREATED", "RUNNING"):
# #         m = """
# #         mutation { bulkOperationCancel { bulkOperation { id status } userErrors { field message } } }
# #         """
# #         data2 = shopify_graphql(m)
# #         # 不强制断言取消成功；只做 best effort
# #         return bid
# #     return None


# # =============================
# # 1) 基础店铺信息查询 ✅ 完成
# # =============================
# @pytest.mark.shopify
# def test_shop_basic_info():
#     """
#     查询店铺基础信息，验证 token / endpoint 是否可用。
#     """
#     q = """
#     query {
#       shop {
#         id
#         name
#         myshopifyDomain
#         plan {
#           displayName
#           partnerDevelopment
#         }
#         primaryDomain { host }
#       }
#     }
#     """
#     data = shopify_graphql(q)
#     shop = data["data"]["shop"]
#     print(json.dumps(shop, indent=2, ensure_ascii=False))
#     assert shop["id"].startswith("gid://shopify/Shop/")



# # =============================
# # 2) 连通性测试（最小查询） ✅ 完成
# # =============================
# @pytest.mark.shopify
# def test_connectivity_minimal():
#     """
#     最小字段连通性测试：只查 shop { id }。
#     """
#     data = shopify_graphql("query { shop { id } }")
#     sid = data["data"]["shop"]["id"]
#     print("Shop ID:", sid)
#     assert sid.startswith("gid://shopify/Shop/")




# # =============================
# # 3) 按标签小查（少量商品）  ✅ 完成
# # =============================
# @pytest.mark.shopify
# def test_query_products_by_tag_small():
#     """
#     使用 Admin GraphQL 的搜索语法：query: "tag:XXX"
#     仅查少量（first: 5），避免拉太多数据。
#     """
#     q = """
#     query($q: String!) {
#       products(first: 5, query: $q) {
#         edges {
#           node {
#             id
#             title
#             tags
#             variants(first: 5) { edges { node { id sku price compareAtPrice } } }
#           }
#         }
#       }
#     }
#     """

#     variables = {"q": f"tag:{TEST_TAG}"}  # CHANGE: 来自你的 settings.SHOPIFY_TAG_FULL_SYNC
#     data = shopify_graphql(q, variables)
#     # ★ 打印整包响应
#     print(json.dumps(data, indent=2, ensure_ascii=False))

#     edges = data["data"]["products"]["edges"]
#     print(f"Found {len(edges)} products by tag={TEST_TAG}")
#     for e in edges:
#         node = e["node"]
#         print("-", node["id"], node["title"])
#     # 不强制要求必须>0（有些店可能没有该标签），只要接口 OK 即通过
#     assert isinstance(edges, list)



# @pytest.mark.shopify
# def test_current_bulk_only():
#     """
#     单测：直接查看当前店铺是否有正在进行/最近的 BulkOperation
#     不做发起/取消，只是读取并输出。
#     """
#     data = current_bulk()
#     # 漂亮地打印整个响应（方便 VSCode Debug 或命令行查看）
#     import json
#     print(json.dumps(data, indent=2, ensure_ascii=False))

#     cur = data.get("data", {}).get("currentBulkOperation")
#     # cur 可能为 None（表示当前没有 bulk），所以这里只做类型/键存在性检查，不强制断言有任务
#     assert "data" in data and "currentBulkOperation" in data["data"]
#     if cur:
#         print(
#             "Current bulk:",
#             cur.get("id"),
#             cur.get("status"),
#             cur.get("objectCount"),
#             cur.get("url"),
#             cur.get("partialDataUrl"),
#         )




# # =============================
# # 4) 发起 Bulk（按标签）
# # =============================
# @pytest.mark.shopify
# def test_bulk_run_products_by_tag():
#     """
#     发起 bulkOperationRunQuery：
#     - 若已有 running 的 bulk，会先尝试取消（best effort）
#     - 然后发起一个按标签的 bulk，拉更大页码（但仍然温和）
#     """
#     # cancel_bulk_if_running()  # 尽量清场，避免 "只能同时运行一个 bulk" 的限制

#     bulk_q = f"""
#     mutation {{
#       bulkOperationRunQuery(
#         query: \"\"\"
#         {{
#           products(first: 250, query: "tag:{TEST_TAG}") {{
#             edges {{
#               node {{
#                 id
#                 title
#                 tags
#                 variants(first: 100) {{
#                   edges {{ node {{ id sku }} }}
#                 }}
#               }}
#             }}
#           }}
#         }}
#         \"\"\"
#       ) {{
#         bulkOperation {{ id status }}
#         userErrors {{ field message }}
#       }}
#     }}
#     """

#     data = shopify_graphql(bulk_q)
#     result = data["data"]["bulkOperationRunQuery"]
#     user_errors = result.get("userErrors") or []
#     if user_errors:
#         raise AssertionError(f"bulkOperationRunQuery userErrors: {json.dumps(user_errors, indent=2, ensure_ascii=False)}")

#     bulk = result["bulkOperation"]
#     print("Bulk started:", json.dumps(bulk, indent=2, ensure_ascii=False))
#     assert bulk["id"].startswith("gid://shopify/BulkOperation/")
#     CTX.bulk_id = bulk["id"]
#     CTX.status = bulk["status"]



# # =============================
# # 5) 轮询 Bulk 直到完成并获取 url
# # =============================
# @pytest.mark.shopify
# def test_bulk_poll_until_done_and_get_url():
#     """
#     轮询 currentBulkOperation(type: QUERY)：
#     - 每 BULK_POLL_INTERVAL 秒查一次（来自 settings）
#     - 最多轮询 ~2 分钟，可按需加大
#     - 完成后保存 url / partialDataUrl 到上下文，供后续测试使用
#     """
#     # CHANGE: 用你的配置作为默认间隔；总尝试次数可按需调大
#     interval_sec = max(2, int(BULK_POLL_INTERVAL))
#     max_attempts = 120 // interval_sec  # 约 2 分钟

#     # 若上一条测试没成功启动，这里仍然尝试查看当前 bulk（可能之前已有任务）
#     attempt = 0
#     final = None
#     while attempt < max_attempts:
#         data = current_bulk()
#         cur = data["data"]["currentBulkOperation"]
#         print("currentBulkOperation:", json.dumps(cur, indent=2, ensure_ascii=False))
#         if cur and cur.get("status") in ("COMPLETED", "FAILED", "CANCELED"):
#             final = cur
#             break
#         if cur and cur.get("status") in ("CREATED", "RUNNING"):
#             attempt += 1
#             time.sleep(interval_sec)
#             continue
#         # 没有当前 bulk（比如启动失败），直接退出
#         break

#     assert final is not None, "没有查询到 bulk 最终状态，请确认上一步已成功发起 bulk 并等待更久一些。"
#     CTX.status = final.get("status")
#     CTX.url = final.get("url")
#     CTX.partial_url = final.get("partialDataUrl")

#     print("Bulk final:", CTX.status, "url:", CTX.url, "partial:", CTX.partial_url)
#     assert CTX.status in ("COMPLETED", "FAILED", "CANCELED")
#     if CTX.status == "COMPLETED":
#         assert CTX.url, "COMPLETED 但未返回 url（不常见），可在后台核查。"

# # =============================
# # 6) 通过 BulkOperation 的 GID 查询详情（node(id: ...））
# # =============================
# @pytest.mark.shopify
# def test_query_bulk_by_gid_via_node():
#     """
#     如果我们存了 BulkOperation 的 GID，可以通过 node(id: ...) 查询详情。
#     实战中常用于后台页面“补查”某次历史 bulk 的状态与下载链接。
#     """
#     # 优先使用第 4 步启动时拿到的 bulk_id；若为空，就从 currentBulkOperation 兜底。
#     bid = CTX.bulk_id
#     if not bid:
#         data = current_bulk()
#         cur = data["data"]["currentBulkOperation"]
#         if cur:
#             bid = cur.get("id")

#     assert bid, "没有可查询的 BulkOperation GID；请先运行发起 bulk 的用例。"

#     q = """
#     query($id: ID!) {
#       node(id: $id) {
#         ... on BulkOperation {
#           id
#           status
#           type
#           createdAt
#           completedAt
#           url
#           partialDataUrl
#         }
#       }
#     }
#     """
#     data = shopify_graphql(q, {"id": bid})
#     node = data["data"]["node"]
#     print(json.dumps(node, indent=2, ensure_ascii=False))
#     assert node and node["id"] == bid
