
from __future__ import annotations
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import time, logging

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.db.model.shopify_jobs import ShopifyUpdateJob
from app.utils.clock import now_utc
from app.utils.backoff import calc_next_delay

logger = logging.getLogger(__name__)


def _has(model, name: str) -> bool:
    return hasattr(model, name)


def lease_jobs(db: Session, limit: int) -> List[ShopifyUpdateJob]:
    """
    抢占一批可执行作业（pending/retry/queued 且 available_at<=now），并标记为 processing。
    使用 FOR UPDATE SKIP LOCKED 避免并发重复消费。
    """
    q = db.query(ShopifyUpdateJob)

    conds = []
    if _has(ShopifyUpdateJob, "status"):
        conds.append(ShopifyUpdateJob.status.in_(["pending", "retry", "queued"]))
    if _has(ShopifyUpdateJob, "available_at"):
        conds.append(ShopifyUpdateJob.available_at <= now_utc())

    if conds:
        q = q.filter(*conds)

    # 排序（存在即用）
    if _has(ShopifyUpdateJob, "priority"):
        q = q.order_by(ShopifyUpdateJob.priority.desc())
    if _has(ShopifyUpdateJob, "created_at"):
        q = q.order_by(ShopifyUpdateJob.created_at.asc())
    if _has(ShopifyUpdateJob, "id"):
        q = q.order_by(ShopifyUpdateJob.id.asc())

    # 抢占
    q = q.with_for_update(skip_locked=True)
    jobs = q.limit(limit).all()

    # 标记 processing + 锁时间
    if jobs:
        now = now_utc()
        for j in jobs:
            if _has(ShopifyUpdateJob, "status"):
                j.status = "processing"
            if _has(ShopifyUpdateJob, "locked_at"):
                j.locked_at = now
        db.commit()

    return jobs



def mark_done(db: Session, job: ShopifyUpdateJob) -> None:
    if _has(ShopifyUpdateJob, "status"):
        job.status = "done"
    if _has(ShopifyUpdateJob, "completed_at"):
        job.completed_at = now_utc()
    if _has(ShopifyUpdateJob, "last_error"):
        job.last_error = None
    # attempts 通常保留历史
    try:
        db.commit()
    except Exception:
        db.rollback()
        # 如果表结构无 status 等字段，降级尝试删除
        try:
            db.delete(job)
            db.commit()
        except Exception:
            db.rollback()
            raise


def mark_fail(
    db: Session,
    job: ShopifyUpdateJob,
    err: Exception,
    max_attempts: int = 5,
    base_sec: int = 10,
    max_sec: int = 1800,
) -> None:
    # 次数+1
    if _has(ShopifyUpdateJob, "attempts"):
        job.attempts = (job.attempts or 0) + 1
        attempts = job.attempts
    else:
        attempts = 1

    # 错误信息
    if _has(ShopifyUpdateJob, "last_error"):
        msg = str(err)
        job.last_error = msg[:2000] + "…" if len(msg) > 2000 else msg

    # 下一次可用时间（指数退避）
    if _has(ShopifyUpdateJob, "available_at"):
        delay = calc_next_delay(attempts, base_sec, max_sec)
        job.available_at = now_utc() + timedelta(seconds=delay)

    # 状态推进
    if _has(ShopifyUpdateJob, "status"):
        job.status = "dead" if attempts >= max_attempts else "retry"

    # 解锁
    if _has(ShopifyUpdateJob, "locked_at"):
        job.locked_at = None

    db.commit()



# todo 有问题需要修改
def enqueue_shopify_jobs(db: Session, jobs: List[Dict[str, Any]]) -> int:
    """
    只负责把待派发作业写入 ShopifyUpdateJob；不做业务判断。
    建议你的唯一键为 (sku_code, op, hash) 或业务允许的约束；下面示例用 do_nothing 防止重复。
    jobs 形如：{"sku": "...", "metafields": [...], "available_at": datetime.utcnow(), ...}
    """
    if not jobs:
        return 0
    rows = []
    now = datetime.utcnow()
    for j in jobs:
        rows.append({
            "id": __import__("uuid").uuid4().hex,
            "payload": json.dumps(j, ensure_ascii=False),
            "status": "pending",
            "available_at": j.get("available_at", now),
            "created_at": now,
            "updated_at": now,
            # 可按你的表结构补充 run_id / trigger 等字段
        })
    stmt = insert(ShopifyUpdateJob).values(rows)
    # 如果你有唯一键可用 on_conflict_do_nothing(index_elements=[...])
    try:
        db.execute(stmt)
    except Exception:
        # 回退到一条条 add 也行；这里先简单抛出
        raise
    return len(rows)