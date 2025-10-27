
# shopify bulk 测试运维接口
# 用来手动触发或查询 Bulk 操作的状态

from __future__ import annotations
from fastapi import APIRouter, HTTPException, Body, Depends
from pydantic import BaseModel, Field
from typing import Optional
from app.orchestration.product_sync.product_sync_task import handle_bulk_finish
from app.integrations.shopify.shopify_client import ShopifyClient
from app.services.auth_service import get_current_user


router = APIRouter(
    prefix="/shopify/bulk", 
    tags=["shopify.bulk"],
    dependencies=[Depends(get_current_user)], 
)


# todo 运维方法重写

# DEFAULT_TAG = "DropshipzoneAU"

# class StartBulkRequest(BaseModel):
#     tag: str = Field(default=DEFAULT_TAG, description="按标签查询商品")
#     allow_if_running: bool = Field(
#         default=True,
#         description="当前已有 bulk 在运行时是否直接复用(true=复用; false=报错）"
#     )


# class StartBulkResponse(BaseModel):
#     status: str
#     bulk_id: Optional[str] = None
#     message: Optional[str] = None



# @router.post("/ops/shopify/webhooks/bulk/ensure")
# def ensure_bulk_finish_webhook(callback_url: str = Body(..., embed=True)):
#     """
#     手动触发：确保 BULK_OPERATIONS_FINISH 的订阅存在并指向 callback_url
#     请求体: { "callback_url": "https://xxxx/webhooks/shopify/bulk_operations/finish" }
#     """
#     client = ShopifyClient()
#     return client.ensure_bulk_finish_webhook(callback_url)


# """
# POST /api/v1/shopify/bulk/products/by-tag
# 触发 Bulk 导出（按 tag)。量级 4万+ 走 Bulk 最稳。
# - 若已存在正在运行的 bulk: 默认直接复用（返回其 id/status)
# - 若发现“已完成且带 url”的 bulk(可能 webhook 丢了）：立即接管后半程
# """
# @router.post("/products/by-tag", response_model=StartBulkResponse)
# def start_products_bulk_by_tag(body: StartBulkRequest):
    
#     cur = current_bulk_operation()
#     current = (cur.get("data") or {}).get("currentBulkOperation") or {}
#     c_status = current.get("status")
#     c_id = current.get("id")
#     c_url = current.get("url")
#     c_cnt = current.get("objectCount")

#     # 情况1：已有运行中的 Bulk
#     if c_status in ("CREATED", "RUNNING"):
#         if not body.allow_if_running:
#             raise HTTPException(status_code=409, detail=f"Bulk already {c_status.lower()}: {c_id}")
#         return StartBulkResponse(status=c_status.lower(), bulk_id=c_id, message="reuse running bulk")

#     # 情况2：发现已完成且带URL（兜底；可能 webhook 未到达）
#     if c_status == "COMPLETED" and c_url:
#         handle_bulk_finish.delay(c_id, c_url, c_cnt)
#         return StartBulkResponse(status="completed", bulk_id=c_id, message="completed: scheduled handle_bulk_finish")

#     # 其它状态（FAILED/CANCELED 或无当前操作）：发起新的 Bulk
#     data = run_bulk_products_by_tag(body.tag)
#     bo = ((data.get("data") or {}).get("bulkOperationRunQuery") or {}).get("bulkOperation") or {}
#     return StartBulkResponse(status=bo.get("status", "unknown").lower(), bulk_id=bo.get("id"))



# class CurrentBulkResponse(BaseModel):
#     id: Optional[str]
#     status: Optional[str]
#     url: Optional[str]
#     objectCount: Optional[int]
#     errorCode: Optional[str]


# '''
# 查询当前 Bulk 的状态(id/status/url/objectCount)
# '''
# @router.get("/current", response_model=CurrentBulkResponse)
# def get_current_bulk():
#     cur = current_bulk_operation()
#     op = (cur.get("data") or {}).get("currentBulkOperation") or {}
#     return CurrentBulkResponse(
#         id=op.get("id"),
#         status=op.get("status"),
#         url=op.get("url"),
#         objectCount=op.get("objectCount"),
#         errorCode=op.get("errorCode"),
#     )
