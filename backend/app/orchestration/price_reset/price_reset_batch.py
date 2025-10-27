
from __future__ import annotations
import logging
from typing import List, Dict, Any

from celery import shared_task
from app.integrations.shopify.shopify_client import ShopifyClient

logger = logging.getLogger(__name__)


# 并行块任务 + 收尾统计
# - update_variant_prices_chunk: 单块写入（可重试），只做外部调用，不碰 DB
# - finalize_price_reset: 汇总块结果
# ⚠️ 把本任务路由到专用队列（如 shopify_write）并设置并发/速率

@shared_task(
    name="app.tasks.price_reset_wed.update_variant_prices_chunk", 
    bind=True, max_retries=3, default_retry_delay=2,
)
def update_variant_prices_chunk(self, metas: List[Dict[str, Any]]) -> dict:
    """
    单块写入任务：把一批 metafields（每个元素一个变体的 KoganAUPrice）写到 Shopify。
    metas 示例见编排层构造；返回 {"size":N, "ok":x, "fail":y}
    """
    client = ShopifyClient()

    try:
        resp = client.metafields_set_batch(metas)
        user_errors = ((resp.get("data") or {}).get("metafieldsSet") or {}).get("userErrors") or []

        if user_errors:
            # 业务错误：通常不适合自动重试（数据/权限/类型问题）
            logger.warning("price_reset.userErrors size=%s sample=%s", len(user_errors), user_errors[:3])

            # 简化：整块失败计数（如需更细粒度，可从 field 中解析 ownerId 对应的变体）
            return {"size": len(metas), "ok": 0, "fail": len(metas), "errors": user_errors[:10]}
        return {"size": len(metas), "ok": len(metas), "fail": 0}
    
    except Exception as e:
        logger.exception("price_reset.chunk.exception size=%s", len(metas))
        # 瞬时错误（网络/429/5xx）走任务级重试
        raise self.retry(exc=e)



@shared_task(name="app.tasks.price_reset_wed.finalize_price_reset")
def finalize_price_reset(results: List[dict]) -> dict:
    """
    汇总任务：统计所有块的结果。
    results: 来自多个 update_variant_prices_chunk 的返回 [{"size":..,"ok":..,"fail":..}, ...]
    """
    total = sum(r.get("size", 0) for r in results)
    ok = sum(r.get("ok", 0) for r in results)
    fail = sum(r.get("fail", 0) for r in results)
    return {"total": total, "ok": ok, "fail": fail}