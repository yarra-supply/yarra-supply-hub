
# app/tasks/dispatch_shopify_task.py
from __future__ import annotations
import os
from typing import Optional

from celery import shared_task
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db.repository.shopify_repo import lease_jobs, mark_done, mark_fail
from app.services.dispatch_runner import run_one_job

# 运行参数（支持 settings 或环境变量）
try:
    from app.core.config import settings
    DISPATCH_LIMIT = getattr(settings, "SHOPIFY_DISPATCH_LIMIT", None)
    MAX_ATTEMPTS   = getattr(settings, "SHOPIFY_MAX_ATTEMPTS", None)
    BACKOFF_BASE   = getattr(settings, "SHOPIFY_BACKOFF_BASE", None)
    BACKOFF_MAX    = getattr(settings, "SHOPIFY_BACKOFF_MAX", None)
except Exception:
    DISPATCH_LIMIT = None
    MAX_ATTEMPTS = None
    BACKOFF_BASE = None
    BACKOFF_MAX  = None


DISPATCH_LIMIT = int(DISPATCH_LIMIT or os.getenv("SHOPIFY_DISPATCH_LIMIT", "200"))
MAX_ATTEMPTS   = int(MAX_ATTEMPTS   or os.getenv("SHOPIFY_MAX_ATTEMPTS", "5"))
BACKOFF_BASE   = int(BACKOFF_BASE   or os.getenv("SHOPIFY_BACKOFF_BASE", "10"))
BACKOFF_MAX    = int(BACKOFF_MAX    or os.getenv("SHOPIFY_BACKOFF_MAX", "1800"))



@shared_task(name="app.tasks.dispatch_shopify_task.kick_dispatch_shopify")
def kick_dispatch_shopify(
    trigger: str = "manual",
    related_run_id: Optional[str] = None,
    batch_count: int = 0,
    limit: Optional[int] = None,
) -> dict:
    """
    扫描一次：抢占→派发→标记
    - 可被频繁调用（被 5.3 每批提交后唤醒；或者定时器轮询）
    """
    db: Session = SessionLocal()
    processed = 0
    succeeded = 0
    failed = 0

    try:
        lease_limit = int(limit or DISPATCH_LIMIT)
        jobs = lease_jobs(db, lease_limit)

        for j in jobs:
            processed += 1
            try:
                run_one_job(j, db)
                mark_done(db, j)
                succeeded += 1
            except Exception as e:
                mark_fail(db, j, e, max_attempts=MAX_ATTEMPTS, base_sec=BACKOFF_BASE, max_sec=BACKOFF_MAX)
                failed += 1

        return {
            "trigger": trigger,
            "related_run_id": related_run_id,
            "processed": processed,
            "succeeded": succeeded,
            "failed": failed,
        }
    finally:
        db.close()



# 兼容旧名（如果历史代码用这个入口）
@shared_task(name="app.tasks.dispatch_shopify_task.dispatch_shopify")
def dispatch_shopify(
    shop_id: Optional[str] = None,
    trigger: str = "manual",
    related_run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict:
    return kick_dispatch_shopify(trigger=trigger, related_run_id=related_run_id, limit=limit)
