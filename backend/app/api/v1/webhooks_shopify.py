# app/api/v1/webhooks_shopify.py

from __future__ import annotations
import hmac, hashlib, base64, json
from fastapi import APIRouter, Request, Response, Header, HTTPException
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.db.session import SessionLocal
from app.core.config import settings

from app.db.model.product import ProductSyncRun
from app.integrations.shopify.shopify_client import ShopifyClient
from app.orchestration.product_sync.product_sync_task import handle_bulk_finish, poll_bulk_until_ready


router = APIRouter(prefix="/webhooks/shopify", tags=["webhooks.shopify"])


'''
正式版（参数解包式）：
  - 函数签名直接接收 x_shopify_hmac_sha256、x_shopify_topic 等 Header。
  - 代码更简洁、语义直观，适合生产。

调试版（手动读取式）：
  - 从 request.headers、await request.body() 手动读取；大量 print()。
  - 易于本地隧道联调（便于观察原始 payload / header），保证5 秒内快速 200。
'''



# =============== 公共：HMAC 校验工具，HMAC 校验, 校验 HMAC（Shopify Webhook 签名） ===============
def _compute_hmac_base64(secret: str, raw_body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _verify_hmac_or_401(provided_hmac_b64: str, raw_body: bytes) -> None:
    if not provided_hmac_b64:  # [ADDED] 缺失即 401，更安全
        raise HTTPException(status_code=401, detail="Missing HMAC")
    
    expected = _compute_hmac_base64(settings.SHOPIFY_WEBHOOK_SECRET, raw_body)
    if not hmac.compare_digest(provided_hmac_b64, expected):
        # 正式环境建议抛 401；调试时可打印后返回 200 以便 Shopify 不连续重试
        raise HTTPException(status_code=401, detail="Invalid HMAC")


# 反查 run_id（用于兜底轮询）
def _run_id_by_bulk(bulk_gid: str) -> str | None:
    db = SessionLocal()
    try:
        row = db.query(ProductSyncRun.id).filter_by(shopify_bulk_id=bulk_gid).first()
        return row[0] if row else None
    finally:
        db.close()



'''
Webhook: bulk_operations/finish
   - 订阅主题: bulk_operations/finish, API: /webhooks/shopify/bulk_operations/finish
   Admin 会推送：{ "admin_graphql_api_id": "gid://shopify/BulkOperation/xxx" }
   非阻塞地读取请求体并校验 HMAC(这两个在 Starlette/FastAPI 里本身就是异步 I/O
   Shopify Bulk 完成回调: bulk_operations/finish Webhook:
     body 示例：
     {
      "admin_graphql_api_id": "gid://shopify/BulkOperation/...",
      "status": "completed",
      "object_count": 12345,
      "url": "https://.../result.jsonl"
     }
    返回给shopify response : 若未完成或失败就直接 200 返回
'''
# =============== A. 正式版：参数解包式（生产推荐） ===============
@router.post("/bulk_operations/finish")
async def bulk_finish(
    request: Request,
    x_shopify_hmac_sha256: str = Header(default=""),
    x_shopify_topic: str = Header(default=""),
    x_shopify_shop_domain: str = Header(default=""),
):
    
    # 1) 先做 HMAC 校验，再看 Topic，避免用任意 Topic 绕过校验
    raw = await request.body()
    _verify_hmac_or_401(x_shopify_hmac_sha256, raw)
    
    # 2) 再校验 Topic（大小写不敏感）
    topic = (x_shopify_topic or "").strip().lower()
    if topic != "bulk_operations/finish":
        # 非本主题；快速 200，避免重试（保持体面）
        return {"ok": True, "ignored": f"topic={x_shopify_topic}"}

    # 3) 解析 payload
    try: 
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    # 🔄 异步处理（不要阻塞 webhook）
    bulk_gid = payload.get("admin_graphql_api_id")      # 拿 Bulk GID: JSON 里拿到 admin_graphql_api_id（BulkOperation 的 GID）
    if not bulk_gid:
        raise HTTPException(status_code=400, detail="Missing bulk id")   # Shopify 官方 payload 里一定会有这个字段；若没有，直接 400


    # 4) 用 GID 查真实 BulkOperation（不完全信任 webhook body）
    client = ShopifyClient()
    try:
        node = client.get_bulk_operation_by_id(bulk_gid)
    except Exception as e:
        # # 失败时安排兜底轮询（不阻塞 webhook；防止 Shopify 重试）
        # try:
        #     run_id = _run_id_by_bulk(bulk_gid)
        #     if run_id:                                # 轮询任务的 task_id 固定，避免重复投递
        #         poll_bulk_until_ready.apply_async(  
        #             args=[run_id],
        #             task_id=f"poll:{bulk_gid}",
        #             countdown=20
        #         )
        # except Exception:
        #     pass             # 兜底失败也不影响 200
        return {"ok": True, "note": f"query error: {type(e).__name__}"}

    status = (node.get("status") or "").upper()  # GraphQL 返回通常是大写
    url = node.get("url")
    # 健壮转换：objectCount 可能是 str/int/None
    try:
        object_count = int(node.get("objectCount")) if node.get("objectCount") is not None else 0
    except Exception:
        object_count = 0

    try:
        root_object_count = (
            int(node.get("rootObjectCount"))
            if node.get("rootObjectCount") is not None
            else None
        )
    except Exception:
        root_object_count = None

    if root_object_count is None:
        root_object_count = object_count
    

    # 5) 标记 webhook 到达时间（仅首次）：首次记录 webhook 到达时间（区分 webhook 触发 vs 轮询触发）只在第一次写入
    db = SessionLocal()
    try:
        run = db.query(ProductSyncRun).filter_by(shopify_bulk_id=bulk_gid).first()
        if run and not getattr(run, "webhook_received_at", None):
            tz = ZoneInfo(getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne"))
            run.webhook_received_at = datetime.now(tz)
            db.commit()
    except Exception:
        db.rollback()   # 不让 DB 异常影响 200
    finally:
        db.close()


    # 6) 未完成或无 URL：快速返回（5 秒规则），由轮询或后续 webhook 兜底
    # todo 为什么需要返回objectCount？
    if status != "COMPLETED" or not url:
        return {
            "ok": True,
            "status": status,
            "objectCount": object_count,
            "rootObjectCount": root_object_count,
        }


    # 7) 已完成：将 URL 投递异步任务（下载/解析/入库）
    try:
        handle_bulk_finish.delay(
            bulk_id=bulk_gid, url=url, root_object_count=root_object_count
        )
    except Exception:
        pass     # 投递失败也不要影响 200，交由轮询兜底


    # 丢给 Celery，接口立即返回 200, Shopify 要求 webhook 回调在 5 秒内返回 200
    return {
        "ok": True,
        "status": status,
        "objectCount": object_count,
        "rootObjectCount": root_object_count,
    }




# 反查 run_id（用于兜底轮询）
def _run_id_by_bulk(bulk_gid: str) -> str | None:
    db = SessionLocal()
    try:
        row = db.query(ProductSyncRun.id).filter_by(shopify_bulk_id=bulk_gid).first()
        return row[0] if row else None
    finally:
        db.close()
