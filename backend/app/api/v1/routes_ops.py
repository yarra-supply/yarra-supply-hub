''' 运营相关的接口（触发全量同步、价格回滚等） '''

from fastapi import APIRouter, Depends, HTTPException
import hmac, hashlib, base64, json
from app.orchestration.product_sync.product_sync_task import sync_start_full, handle_bulk_finish
from app.orchestration.price_reset.price_reset import kick_price_reset
# 导入 poll 与 scheduler 方法
from app.orchestration.product_sync.product_sync_task import poll_bulk_until_ready
from app.orchestration.product_sync.scheduler import (
    schedule_chunks_streaming, schedule_chunks_from_manifest
)
from app.services.auth_service import get_current_user

from app.db.session import SessionLocal
from app.db.model.product import ProductSyncRun, ProductSyncChunk
from app.core.config import settings

# 如果要触发 Celery 任务，可再解注释下一行：
# from app.tasks.dispatch_shopify import dispatch_now
# from app.tasks.retry_sweeper import retry_failed_jobs


router = APIRouter(
    prefix="/ops", 
    tags=["ops"],
    dependencies=[Depends(get_current_user)], 
)


#=================== 商品同步流程运营相关接口 ==================== #

''' 触发全量同步 '''
@router.post("/full-sync")
def ops_start_full_sync():
    # 直接触发一次；生产里可根据权限/参数控制 tag
    task_id = sync_start_full.delay("DropShippingZone").id
    return {"task_id": task_id}


''' 触发价格回滚 '''
@router.post("/reset-price")
def trigger_price_reset():
    task_id = revert_special_prices.delay().id
    return {"task_id": task_id}


# 异常运维接口
# 一个接口搞定：
#     - 保证拿到 bulk_url → 若无 manifest 则切片并建清单 → 若有清单只重投 pending/failed → 收口
@router.post("/sync/runs/{run_id}/resume") 
def ops_resume_run(run_id: str):

    db = SessionLocal()

    try:
        run = db.get(ProductSyncRun, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="run not found")

        # 1) 保证拿到 bulk_url
        if not run.shopify_bulk_id and not run.shopify_bulk_url:
            # 没有 bulk 的 run 基本无法续跑，建议重新发起一次全量
            raise HTTPException(status_code=400, detail="run has no bulk_id/url, please start a new full sync")

        if run.shopify_bulk_id and not run.shopify_bulk_url:
            # 触发一次 poll（确定性 task_id，避免重复 poller）
            task_id = f"poll:{run.shopify_bulk_id}"
            poll_bulk_until_ready.apply_async(args=[run_id], task_id=task_id, countdown=0)
            return {"run_id": run_id, "action": "polling", "detail": "bulk url not ready, polling scheduled"}


        # 2) 如果没有 manifest（首次切片），则走流式切片 + 建清单 + 调度所有分片
        has_manifest = db.query(ProductSyncChunk.id).filter(ProductSyncChunk.run_id == run_id).limit(1).first() is not None
        if not has_manifest:
            task_or_id = schedule_chunks_streaming(run_id, run.shopify_bulk_url)
            return {"run_id": run_id, "action": "chunking_all", "task": task_or_id}


        # 3) 有 manifest：只重投 pending/failed 分片，并创建 chord 收口到 finalize_run
        task_or_id = schedule_chunks_from_manifest(run_id, statuses=("pending", "failed"))
        return {"run_id": run_id, "action": "chunking_resume", "task": task_or_id}

    finally:
        db.close()



@router.post("/dispatch-now")
def ops_dispatch_now():
    # dispatch_now.delay("default")   # 真要触发再解注释
    return {"ok": True}


@router.post("/retry-now")
def ops_retry_now():
    # retry_failed_jobs.delay()
    return {"ok": True}


# 触发shopify bulk查询商品，结果计算完成回调通知
# @router.post("/webhooks/shopify/bulk_finish")
# async def shopify_bulk_finish(request: Request, x_shopify_hmac_sha256: str = Header(None)):
#     body = await request.body()
#     mac = hmac.new(settings.shopify_webhook_secret.get_secret_value().encode(), body, hashlib.sha256).digest()
#     calc = base64.b64encode(mac).decode()
#     if calc != (x_shopify_hmac_sha256 or ""):
#         raise HTTPException(status_code=401, detail="Invalid HMAC")

#     payload = json.loads(body)
#     status = (payload.get("status") or "").upper()
#     if status != "COMPLETED":
#         return {"ok": True, "status": status}

#     bulk_id = payload.get("admin_graphql_api_id") or payload.get("id")
#     url = payload.get("url")
#     obj_cnt = payload.get("object_count")
#     if not (bulk_id and url):
#         raise HTTPException(status_code=400, detail="missing bulk_id/url")

#     handle_bulk_finish.delay(bulk_id, url, obj_cnt)
#     return {"ok": True}