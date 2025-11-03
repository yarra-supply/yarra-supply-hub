
"""面向 Admin GraphQL 的轻量 Client, 只放和本模块强相关的方法"""
from __future__ import annotations

import json, time, logging, requests
from typing import Any, Dict, Generator, Optional
from requests import HTTPError, Timeout, RequestException

from app.core.config import settings
from app.integrations.shopify.graphql_queries import (
    BULK_PRODUCTS_BY_TAG_AND_STATUS,
    # BULK_PRODUCTS_BY_TAG_AND_STATUS_TEST_LIMIT_20,
    PRODUCTS_BY_TAG_AND_STATUS,
    _LIST_WEBHOOKS,
    _CREATE_WEBHOOK,
    _DELETE_WEBHOOK,
    escape_tag_for_query,
    BULK_PRODUCTS_BY_TAG_AND_STATUS_LIMITED,
)


logger = logging.getLogger(__name__)


# ---------------- 基础：端点 & 认证 ----------------

# 统一GraphQL Admin API 入口: graphql.json 表示走 GraphQL Admin API
def _graphql_endpoint() -> str:
    # 用 myshopify 域名 + 版本拼接 GraphQL Admin API 端点
    return f"https://{settings.SHOPIFY_SHOP}/admin/api/{settings.SHOPIFY_API_VERSION}/graphql.json"


# 全局 API 端点
def _auth_headers() -> dict:
    # 统一构造认证头。兼容 SecretStr 或 str。
    token = settings.SHOPIFY_ADMIN_TOKEN

    if hasattr(token, "get_secret_value"):
        token = token.get_secret_value()

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
        # 可选：统一 UA，便于排查
        "User-Agent": f"YarraSupplyHub/ShopifyClient (+python)",
    }
    # [CHANGED] 可选：透传 X-Request-Id 便于串联日志（如果 settings 配置了）
    # req_id = getattr(settings, "REQUEST_ID", None)
    # if req_id:
    #     headers["X-Request-Id"] = str(req_id)
    return headers



class ShopifyClient:

    '''
    通用 GraphQL POST（带日志 + 重试 + 埋点) 调用 Admin GraphQL 的公共逻辑
        - 统一 headers、json 负载、超时、HTTP 错误与 GraphQL 顶层 errors 处理, 用 json= 发送
        - 这样其他的GraphQL 方法（run_bulk_products_by_tag、get_bulk_operation_by_id等）都复用
        - 返回完整 data（上层自己从 data[...] 取需要的节点）
        异常处理: 
           1) 对 HTTP 5xx/网络异常做指数退避重试(raise_for_status 处理 HTTP 错)
           2) 对 HTTP 4xx不重试（直接抛）
           3) 店铺偶发 429 也走一次轻微 backoff 重试
           4) 对 顶层 GraphQL errors直接抛 RuntimeError
    '''
    def _post_graphql(
        self, 
        query: str, 
        variables: Optional[dict] = None, 
        *,
        timeout: Optional[int] = None,
        op_name: str = "",   # 便于日志区分：例如 "bulkOperationRunQuery" / "currentBulkOperation"
    ) -> dict:

        # 1. 参数准备
        timeout = timeout or getattr(settings, "SHOPIFY_HTTP_TIMEOUT", 30)
        max_retries = max(0, int(getattr(settings, "SHOPIFY_HTTP_RETRIES", 3)))
        backoff_ms = max(50, int(getattr(settings, "SHOPIFY_HTTP_BACKOFF_MS", 200)))

        payload = {"query": query, "variables": variables or {}}
        # 不打印 query 全文，避免日志过大/敏感；仅打 op_name / 变量键
        safe_vars_keys = list(payload["variables"].keys())

        for attempt in range(max_retries + 1):
            start = time.perf_counter()
            try:
                resp = requests.post(
                    _graphql_endpoint(),
                    headers=_auth_headers(),
                    json=payload,
                    timeout=timeout,
                )
                latency_ms = int((time.perf_counter() - start) * 1000)

                # HTTP 层错误
                try:
                    resp.raise_for_status()
                except HTTPError as e:
                    status = resp.status_code

                    # --- 新增：对 429 限流做退避重试 ---
                    if status == 429 and attempt < max_retries:
                        retry_after = resp.headers.get("Retry-After")
                        # Retry-After 可能是秒数；没有就按指数退避
                        try:
                            sleep_s = max(0.1, float(retry_after))
                        except (TypeError, ValueError):
                            sleep_s = (backoff_ms / 1000.0) * (2 ** attempt)
                        logger.warning(
                            "shopify.graphql.429_throttled op=%s latency_ms=%s attempt=%s/%s retry_after=%s",
                            op_name, latency_ms, attempt, max_retries, retry_after)
                        time.sleep(sleep_s)
                        continue
                    
                    # 4xx 直接失败；5xx 允许重试
                    logger.warning(
                        "shopify.graphql.http_error op=%s status=%s latency_ms=%s attempt=%s/%s",
                        op_name, status, latency_ms, attempt, max_retries)
                    
                    if 400 <= status < 500 or attempt == max_retries:
                        raise
                    if 500 <= status < 600 and attempt < max_retries:
                        time.sleep((backoff_ms / 1000.0) * (2 ** attempt))
                        continue
                    raise

                # 解析 JSON
                try:
                    data = resp.json()
                except ValueError:
                    # 非 JSON 响应：若还有重试机会，退避后重来
                    if attempt < max_retries:
                        logger.warning("shopify.graphql.non_json op=%s attempt=%s/%s", op_name, attempt, max_retries)
                        time.sleep((backoff_ms / 1000.0) * (2 ** attempt))
                        continue
                    raise RuntimeError(f"GraphQL response is not JSON: status={resp.status_code}")

                # 顶层 GraphQL errors 直接视为硬错误（通常不可恢复）
                if data.get("errors"):
                    logger.error(
                        "shopify.graphql.gql_errors op=%s latency_ms=%s attempt=%s/%s errors=%s",
                        op_name, latency_ms, attempt, max_retries, data["errors"])
                    # 顶层 errors 多为语法/权限问题，直接抛出不重试
                    raise RuntimeError(f"GraphQL top-level errors: {data['errors']}")

                # 成功
                logger.info("shopify.graphql.ok op=%s latency_ms=%s attempt=%s vars=%s",
                    op_name, latency_ms, attempt, safe_vars_keys)
                return data

            except (Timeout,) as e:
                latency_ms = int((time.perf_counter() - start) * 1000)
                logger.warning("shopify.graphql.timeout op=%s latency_ms=%s attempt=%s/%s",
                    op_name, latency_ms, attempt, max_retries)
                if attempt == max_retries:
                    raise
                time.sleep((backoff_ms / 1000.0) * (2 ** attempt))

            except RequestException as e:
                # 其他网络层/连接异常：允许重试
                latency_ms = int((time.perf_counter() - start) * 1000)
                logger.warning("shopify.graphql.request_exception op=%s latency_ms=%s attempt=%s/%s err=%s",
                    op_name, latency_ms, attempt, max_retries, type(e).__name__)
                if attempt == max_retries:
                    raise
                time.sleep((backoff_ms / 1000.0) * (2 ** attempt))
    
    
    # 基础连通性探测（便于本地先测 token/域名/版本是否正确）test ✅
    def ping(self) -> dict:
        q = """
        {
          shop {
            name
            myshopifyDomain
            plan { displayName }
          }
        }
        """.strip()
        return self._post_graphql(q, op_name="shop.ping")
    

    # test ✅
    def run_bulk_products_by_tag(
        self,
        tag: str,
        *,
        products_first: Optional[int] = None,
        variants_first: Optional[int] = None,
        status: str = "active",
    ) -> dict:
        """
        发起 bulkOperationRunQuery，按标签（及可选状态）导出商品与变体。

        products_first/variants_first 允许在测试环境下限制导出的数量，
        若不提供则由 Shopify 返回全部匹配结果。
        """
        safe_tag = escape_tag_for_query(tag)
        search_terms = [f"tag:{safe_tag}"]
        if status:
            status = status.strip()
            if status:
                search_terms.append(f"status:{status}")

        search_filter = " ".join(search_terms)
        filter_literal = json.dumps(search_filter)

        products_parts = [f"query: {filter_literal}"]
        if products_first is not None:
            products_limit = max(1, int(products_first))
            products_parts.insert(0, f"first: {products_limit}")
        products_args = ", ".join(products_parts)
        expected_marker = f"products({products_args})"


        variants_args = ""
        if variants_first is not None:
            variants_limit = max(1, int(variants_first))
            variants_args = f"(first: {variants_limit})"

        # 0) 若已有一个 Bulk 在跑，仅在确实是同一个任务时复用
        try:
            cur = self.current_bulk_operation() or {}
            if cur.get("status") in ("CREATED", "RUNNING"):
                cur_query = cur.get("query") or ""
                if expected_marker in cur_query:
                    self._last_bulk_operation_id = cur.get("id")
                    return cur
                raise RuntimeError(
                    f"Another bulk operation is already {cur.get('status')}: {cur.get('id')}"
                )
        except Exception:
            # 不阻断流程：查询 current 失败时继续尝试发起
            pass

        # 1) 生成“内层查询”文本
        query_doc = BULK_PRODUCTS_BY_TAG_AND_STATUS % {
            "products_args": products_args,
            "variants_args": variants_args,
        }
 
        # ====== 生成测试版查询：限制 20 条，便于本地调试 =====
        # query_doc_test = BULK_PRODUCTS_BY_TAG_AND_STATUS_TEST_LIMIT_20 % {
        #     "filter": filter_literal,
        #     "variants_args": variants_args,
        # }
        # self._last_bulk_query_docs = {"full": query_doc, "test": query_doc_test}

        # use_test_query = bool(getattr(settings, "SHOPIFY_BULK_TEST_MODE", True))
        # query_to_run = query_doc_test if use_test_query else query_doc
        # if use_test_query:
        #     logger.info(
        #         "shopify.bulk.test_mode_enabled tag=%s limit=20 variants=%s",
        #         tag,
        #         variants_args or "all",
        #     )
        # ====== test end =====


        # 2) 真正触发 bulk 导出: 外层 mutation（把内层查询字符串作为变量传入）
        mutation = """
        mutation RunBulk($query: String!) {
           bulkOperationRunQuery(query: $query) {
            bulkOperation { id status }
            userErrors { field message code }
          }
        }
        """.strip()

        # 3) 针对 userErrors 的“业务级重试”
        max_attempts = max(1, int(getattr(settings, "SHOPIFY_BULK_START_RETRIES", 4)))
        base_backoff = max(0.2, int(getattr(settings, "SHOPIFY_HTTP_BACKOFF_MS", 200)) / 1000.0)

        for attempt in range(max_attempts):
            data = self._post_graphql(
                mutation,
                {"query": query_doc},
                timeout=30,
                op_name="bulkOperationRunQuery",
            )
            payload = (data.get("data") or {}).get("bulkOperationRunQuery") or {}
            user_errors = payload.get("userErrors") or []

            if not user_errors:
                bulk_op = payload.get("bulkOperation")
                if not bulk_op:
                    raise RuntimeError("bulkOperationRunQuery missing bulkOperation payload")
                self._last_bulk_operation_id = bulk_op["id"]
                return bulk_op  # 成功

            # 4) 有 userErrors → 判断是否可恢复
            msgs = [str(e.get("message") or "") for e in user_errors]
            codes = {str(e.get("code") or "") for e in user_errors}

            # 情况 A：已存在运行中的 bulk → 若是同一个任务则复用，否则报错
            if any("already in progress" in m.lower() for m in msgs):
                cur = self.current_bulk_operation() or {}
                cur_query = cur.get("query") or ""
                if cur.get("id") and expected_marker in cur_query:
                    self._last_bulk_operation_id = cur.get("id")
                    return cur
                raise RuntimeError(
                    f"Shopify reports an existing bulk operation in progress: {cur.get('id')}"
                )

            # 情况 B：被限流/暂时性错误 → 退避重试
            throttled = ("THROTTLED" in codes) or any("throttle" in m.lower() for m in msgs)
            transient = throttled or ("INTERNAL_SERVER_ERROR" in codes)
            if transient and attempt < max_attempts - 1:
                sleep_s = base_backoff * (2 ** attempt)
                logger.warning(
                    "shopify.bulk.start_retry op=bulkOperationRunQuery attempt=%s/%s sleep=%.2fs codes=%s msgs=%s",
                    attempt + 1,
                    max_attempts,
                    sleep_s,
                    list(codes),
                    msgs[:1],
                )
                time.sleep(sleep_s)
                continue

            # 其他硬错误 → 直接失败
            raise RuntimeError(f"bulkOperationRunQuery userErrors: {user_errors}")

        # 理论不会走到这里
        raise RuntimeError("bulkOperationRunQuery failed after retries")
    

    # test ✅
    """
        普通 GraphQL 查询：按标签拉取少量商品（默认 10 个）。
        返回 products connection（含 edges/pageInfo）。
    """
    def query_products_by_tag(
        self,
        tag: str,
        *,
        first: int = 10,
        variants_first: int = 50,
    ) -> dict:
        
        safe_tag = escape_tag_for_query(tag)
        search_filter = f"tag:{safe_tag} status:active"
        first = max(1, int(first))
        variants_first = max(1, int(variants_first))

        variables = {
            "query": search_filter,
            "first": first,
            "variantsFirst": variants_first,
        }

        data = self._post_graphql(
            PRODUCTS_BY_TAG_AND_STATUS,
            variables,
            op_name="productsByTag",
        )
        return (data.get("data") or {}).get("products") or {}

 
    # test ✅
    # ---------- webhook + 轮询使用：Bulk：通过 GID 取结果URL（用于 webhook 拿到 id 时） ----------
    def get_bulk_operation_by_id(self, bulk_gid: str) -> dict:
        """
        通过 BulkOperation 的 GID 查询详情（在 webhook 中用到）：
        返回：{id,status,objectCount,url,createdAt,completedAt}
        - 当任务完成(COMPLETED)时，url 才会非空
        """
        query = """
        query BulkById($id: ID!) {
          node(id: $id) {
            __typename
            ... on BulkOperation {
              id
              status
              objectCount
              rootObjectCount
              url
              createdAt
              completedAt
            }
          }
        }
        """

        data = self._post_graphql(query, {"id": bulk_gid}, op_name="bulkOperation.node")

        node = ((data.get("data") or {}).get("node") or {})

        # 不是 BulkOperation（极少数情况，比如 GID 错了）
        if node.get("__typename") != "BulkOperation":
            return {}
        
        # 规范化计数（Shopify 返回字符串）
        for key in ("objectCount", "rootObjectCount"):
            value = node.get(key)
            if isinstance(value, str):
                try:
                    node[key] = int(value)
                except ValueError:
                    pass
        
        # 删除 __typename，保持对外返回干净
        node.pop("__typename", None)
        return node
    

    # test ✅
    # ---------- Bulk：查询shopify当前的bulk operation ----------
    """
        查询当前店铺的 bulkOperation（如果有）。
        返回：{id,status,objectCount,url,createdAt,completedAt}
    """
    def current_bulk_operation(self) -> dict:
        
        query = """
        {
          currentBulkOperation {
             id
             status
             type
             objectCount
             rootObjectCount
             query
             url
             createdAt
             completedAt
             errorCode
          }
        }
        """.strip()

        data = self._post_graphql(query, op_name="currentBulkOperation")

        node = (data.get("data") or {}).get("currentBulkOperation")
        if not node:
            return {}    # 没有正在执行/最近的 bulk
        
        # 把计数字符串规范化为 int（若失败就保留原值）
        for key in ("objectCount", "rootObjectCount"):
            value = node.get(key)
            if isinstance(value, str):
                try:
                    node[key] = int(value)
                except ValueError:
                    pass
        return node



    # ---------- Bulk：下载 JSONL ----------
    """
        以流式方式下载 JSONL，每次 yield 一行字符串。
        供上游切片器（iter_variant_from_bulk）边读边切分片
    """
    # test ✅ 
    def download_jsonl_stream(self, url: str) -> Generator[str, None, None]:
        timeout = getattr(settings, "BULK_DOWNLOAD_TIMEOUT", 120)
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            for chunk  in r.iter_lines(decode_unicode=True):
                if not chunk :
                    continue
                yield chunk 

    

    # ⚠️ ---------- 新环境/新店铺初始化 ⚠️ Webhook 订阅：确保存在/对齐回调地址  ----------
    """
    幂等创建/校准工具：
       - 确保店铺已经订阅了 Shopify Admin GraphQL 的 BULK_OPERATIONS_FINISH Webhook, 并且回调地址就是提供的 callback_url
       - 用途：新环境/新店铺初始化：一键创建或修正 Webhook 订阅
       - 若已存在同 callback → 返回 {"action":"noop", ...}
       - 若存在不同 callback → 可选删除后重建（delete_others=True）
       - 若不存在 → 创建并返回 {"action":"created", ...}
    """
    # todo delete_others 默认 True 是否合适？先设置为false
    def ensure_bulk_finish_webhook(self, callback_url: str, *, 
                                   delete_others: bool = False) -> Dict[str, Any]:

        topic = "BULK_OPERATIONS_FINISH"

        # 1) 列出当前 topic 的订阅
        q = self._post_graphql(_LIST_WEBHOOKS, {"first": 50, "topic": topic}, op_name="webhook.list")
        edges = (q.get("data", {}).get("webhookSubscriptions") or {}).get("edges", [])
        existing = []
        for e in edges:
            node = e.get("node") or {}
            ep = node.get("endpoint") or {}
            cb = ep.get("callbackUrl") if ep.get("__typename") == "WebhookHttpEndpoint" else None
            existing.append({"id": node.get("id"), "callbackUrl": cb})

        # 2) 已存在相同 callback → 返回 noop
        for it in existing:
            if it.get("callbackUrl") == callback_url:
                return {"action": "noop", "id": it["id"], "topic": topic, "callbackUrl": callback_url}

        # 3) 删除其它 callback（避免重复）
        # deleted = []
        # if delete_others:
        #     for it in existing:
        #         if it.get("id"):
        #             d = self._post_graphql(_DELETE_WEBHOOK, {"id": it["id"]}, op_name="webhook.delete")
        #             ue = d["data"]["webhookSubscriptionDelete"]["userErrors"]
        #             if ue:
        #                 raise RuntimeError(f"delete userErrors: {ue}")
        #             deleted.append(d["data"]["webhookSubscriptionDelete"]["deletedWebhookSubscriptionId"])

        # 4) 创建新订阅
        c = self._post_graphql(_CREATE_WEBHOOK, {"topic": topic, "cb": callback_url}, op_name="webhook.create")
        ue = c["data"]["webhookSubscriptionCreate"]["userErrors"]
        if ue:
            raise RuntimeError(f"create userErrors: {ue}")
        node = c["data"]["webhookSubscriptionCreate"]["webhookSubscription"]
        return {
            "action": "created", 
            "id": node["id"], 
            "topic": node["topic"], 
            "callbackUrl": callback_url, 
            # "deleted": deleted
        }
    

    # todo 和bulk区分py文件，是否可以通用？
    '''
    批量写入：metafieldsSet
       - 场景1: wed reset price
       - 场景2: 批量更新所有字段
    '''
    def metafields_set_batch(self, metas: list[dict]) -> dict:
        """
         GraphQL mutation: metafieldsSet
         metas: 列表，每项形如：
         {
             "ownerId": "gid://shopify/ProductVariant/123",
             "namespace": "custom",
             "key": "KoganAUPrice",
             "type": "number_decimal",          # 或 single_line_text_field（取决于你的后台 metafield 定义）
             "value": "79.99"
           }
        注意：
          - Shopify 对 mutation 的输入长度有整体限制，建议业务侧自行分块（上层已按 _SHOPIFY_WRITE_CHUNK 分块）
          - 失败详情会出现在 data.metafieldsSet.userErrors
        """
        
        mutation = """
        mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
              metafieldsSet(metafields: $metafields) {
                metafields { id key namespace }
                 userErrors { field message code }
              }
        }
        """.strip()
        
        return self._post_graphql(
            mutation,
            {"metafields": metas},
            timeout=int(getattr(settings, "SHOPIFY_HTTP_TIMEOUT", 30)),
            op_name="metafieldsSet",
        )
