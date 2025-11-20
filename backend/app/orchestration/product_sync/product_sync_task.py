
from __future__ import annotations
import logging, json, time
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Callable, Optional
import os
import requests
import math

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from app.utils.attrs_hash import calc_attrs_hash_current

from app.core.config import settings
from app.db.session import SessionLocal
from sqlalchemy import func
from app.db.model.product import ProductSyncRun, ProductSyncChunk

from app.integrations.shopify.shopify_client import ShopifyClient
from app.integrations.dsz import (
    get_products_by_skus_with_stats, 
    normalize_dsz_product,
    get_zone_rates_by_skus,
)
from app.integrations.shopify.payload_utils import normalize_sku_payload
from app.repository.product_repo import (
    load_existing_by_skus, bulk_upsert_sku_info, save_candidates,
    load_variant_ids_by_skus, mark_chunk_running, mark_chunk_succeeded, mark_chunk_failed,
    collect_shopify_skus_for_run, purge_sku_info_absent_from,
)
from app.orchestration.product_sync.scheduler import (
    schedule_chunks_streaming,
    schedule_chunks_from_manifest,
    SYNC_CHUNK_SKUS as SCHED_CHUNK_SIZE,
)
from app.orchestration.product_sync.chunk_enricher import enrich_shopify_snapshot
from app.orchestration.product_sync.utils import diff_snapshot, build_candidate_rows
from app.orchestration.freight_calculation.freight_task import kick_freight_calc
from sqlalchemy.sql.elements import BindParameter, ClauseElement
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import QueryableAttribute

# 可选 Redis 锁
try:
    import redis  # redis-py
except Exception:
    redis = None


logger = logging.getLogger(__name__)
_shopify = ShopifyClient()  # 单实例即可

SYNC_CHUNK_SKUS = SCHED_CHUNK_SIZE


"""
  调试开关：True 时所有子任务在当前进程内同步执行。
"""
def _inline_tasks_enabled() -> bool:
    return bool(getattr(settings, "SYNC_TASKS_INLINE", True))



"""
    Celery 入口：创建 run 并异步投递后续轮询。
"""
@shared_task(name="app.orchestration.product_sync_task.sync_start_full")
def sync_start_full() -> str:
    return _sync_start_full_logic(inline=_inline_tasks_enabled())


"""
    调试入口：在当前进程同步执行完整流程。
    poll_kwargs 将透传给 poll_bulk_until_ready_inline（例如 max_attempts / sleep）。
"""
def sync_start_full_inline(**poll_kwargs: Any) -> str:
    return _sync_start_full_logic(inline=True, poll_kwargs=poll_kwargs or None)



# ========================== 任务触发流程 ==========================
"""
提交 Shopify Bulk 任务入口
    1) 写入 run 记录
    2) 发起 Shopify Bulk
    3) 安排兜底轮询
"""
def _sync_start_full_logic(*, inline: bool, poll_kwargs: Dict[str, Any] | None = None) -> str:

    logger.info("sync_start_full_logic start inline=%s", inline)

    # check running 的run_id, 继续当前run_id执行
    resumed_run_id = _resume_running_run_if_any(inline=inline, poll_kwargs=poll_kwargs)
    if resumed_run_id:
        logger.info("resume existing run=%s instead of starting new one", resumed_run_id)
        return resumed_run_id


    TAG = getattr(settings, "SHOPIFY_TAG_FULL_SYNC", "DropshipzoneAU")
    db = SessionLocal()
    run_id: Optional[str] = None

    try:
        # 1. 建立 run 记录
        run = ProductSyncRun(status="running", run_type="full_sync")
        db.add(run); db.commit(); db.refresh(run)
        run_id = run.id

        # 2. 发起 Shopify Bulk 内部已包含：429/网络重试、并发合流、可恢复 userErrors 的退避
        try:
            node = _shopify.run_bulk_products_by_tag(TAG)
        except Exception as e:
            run.status = "failed"
            db.commit()
            logger.exception("failed to start shopify bulk: run=%s err=%s", run.id, e)
            raise
        logging.info("started shopify bulk: run=%s node=%s", run.id, node)

        if not node or not node.get("id"):
            run.status = "failed"
            db.commit()
            return f"failed to start bulk: {node}"

        # 3. 保存 bulk_id 并安排兜底轮询
        run.shopify_bulk_id = node["id"]   # 直接取 id
        db.commit()

        # 4. 40s 后开始兜底轮询（Webhook 会更快进入后半程）
        if inline:
            poll_kwargs = poll_kwargs or {}
            poll_bulk_until_ready_inline(run.id, **poll_kwargs)
        else:
            task_id = f"poll:{run.shopify_bulk_id}"
            poll_bulk_until_ready.apply_async(
                args=[run.id], countdown=40, task_id=task_id
            )

        return run.id
    finally:
        db.close()
        logger.info("sync_start_full_logic end inline=%s run_id=%s", inline, run_id)




"""
    调试入口：串行执行轮询流程，直到成功或超过最大尝试次数。
"""
def poll_bulk_until_ready_inline(
    run_id: str | None,
    *,
    max_attempts: int | None = None,
    sleep: Callable[[float], None] = time.sleep,
    default_retry_delay: int = 60,
) -> Any:
    
    attempt = 0
    while True:
        should_retry, delay, result = _poll_bulk_until_ready_step(
            run_id,
            attempt,
            inline=True,
            default_retry_delay=default_retry_delay,
        )
        if not should_retry:
            return result

        attempt += 1
        if max_attempts is not None and attempt > max_attempts:
            raise RuntimeError("poll_bulk_until_ready_inline exceeded max_attempts")

        sleep(delay or default_retry_delay)



"""
真实流程入口：串行执行轮询流程，直到成功或超过最大尝试次数。
"""
@shared_task(
    name="app.orchestration.product_sync_task.poll_bulk_until_ready",
    bind=True, max_retries= 30, default_retry_delay= 60  # 最多30次，每次间隔60s → 最长约 30 分钟（指数退避）实际测试 3min
)
# todo test retry
def poll_bulk_until_ready(self, run_id: str):

    attempt = getattr(self.request, "retries", 0)
    should_retry, delay, result = _poll_bulk_until_ready_step(
        run_id,
        attempt,
        inline=_inline_tasks_enabled(),
        default_retry_delay=int(getattr(self, "default_retry_delay", 60)),
    )

    if should_retry:
        raise self.retry(countdown=delay or self.default_retry_delay)

    return result




"""
   查询bulk_operation task status
   执行一次轮询逻辑，返回 (should_retry, delay_seconds, result)。
"""
def _poll_bulk_until_ready_step(
    run_id: str | None,
    attempt: int,
    *,
    inline: bool,
    default_retry_delay: int = 60,
) -> tuple[bool, int | None, Any]:
    
    logger.info("query bulk operation status start run_id=%s attempt=%s", run_id, attempt)

    db = SessionLocal()

    try:
        if not run_id:
            info = _shopify.current_bulk_operation() or {}
            status = info.get("status")
            url = info.get("url")
            root_object_count = info.get("rootObjectCount")
            op_id = info.get("id")

            if status == "COMPLETED" and url:
                result = _dispatch_handle_bulk_finish(
                    op_id, url, root_object_count, inline=inline
                )
                return (False, None, result or "handled via polling(current)")

            return (True, _poll_retry_delay(attempt, default_retry_delay), None)


        run = db.get(ProductSyncRun, run_id)
        if not run:
            return (False, None, "no run")
        if run.shopify_bulk_url:
            return (False, None, "already has url")

        if run.shopify_bulk_id:
            info = _shopify.get_bulk_operation_by_id(run.shopify_bulk_id) or {}
        else:
            info = _shopify.current_bulk_operation() or {}

        status = info.get("status")
        url = info.get("url")
        root_object_count = info.get("rootObjectCount")
        op_id = info.get("id")

        run.shopify_bulk_status = status
        db.commit()

        if status in ("FAILED", "CANCELED"):
            run.status = "failed"
            db.commit()
            return (False, None, f"bulk {status.lower()}")

        if status == "COMPLETED" and url:
            bulk_id = run.shopify_bulk_id or op_id
            result = _dispatch_handle_bulk_finish(
                bulk_id, url, root_object_count, inline=inline
            )
            logger.info("query bulk operation status finish run_id=%s attempt=%s", run_id, attempt)
            return (False, None, result or "handled via polling")

        return (True, _poll_retry_delay(attempt, default_retry_delay), None)

    finally:
        db.close()


"""
与 Celery 任务保持一致的指数退避延迟计算。
"""
def _poll_retry_delay(attempt: int, default_retry_delay: int = 60) -> int:
    delay = int(default_retry_delay * (1.2 ** max(0, attempt)))
    return min(60, max(1, delay))



"""
    根据执行模式触发 handle_bulk_finish。
"""
def _dispatch_handle_bulk_finish(
    bulk_id: str, url: str, root_object_count: Any | None, *, inline: bool
):
    # 测试模式-同步触发
    if inline or _inline_tasks_enabled():
        return handle_bulk_finish_inline(bulk_id, url, root_object_count)
    
    # 真实流程-celery任务异步触发
    handle_bulk_finish.apply_async(args=[bulk_id, url, root_object_count],
        task_id=f"finish:{bulk_id}", retry=False,
    )
    return "handle_bulk_finish dispatched"




'''
  最终处理 Shopify 返回 URL、调度分片的 Celery 任务
'''
@shared_task(
    name="app.orchestration.product_sync_task.handle_bulk_finish",
    bind=True,
    max_retries=0,
    default_retry_delay=15,
    retry_backoff=True,
    acks_late=True,
    retry_jitter=True,
    autoretry_for=(Exception,),
)
def handle_bulk_finish(self, bulk_id: str, url: str, root_object_count: int | None = None):
    try:
        return _handle_bulk_finish_logic(bulk_id, url, root_object_count)
    except SoftTimeLimitExceeded as e:
        logger.warning(
            "soft time limit exceeded, will retry: bulk_id=%s err=%s",
            bulk_id,
            e,
        )
        raise self.retry(exc=e, countdown=20)



"""
    调试入口：同步执行 handle_bulk_finish 逻辑。
"""
def handle_bulk_finish_inline(
    bulk_id: str, url: str, root_object_count: int | None = None
):
    return _handle_bulk_finish_logic(bulk_id, url, root_object_count)





# ========================== 接收shopify回调 后半段处理流程 ===========================
'''
Webhook/轮询：拿到 Bulk URL → 流式切片调度（5k/片）→ chord 汇总 
   - webhook/轮询 触发, 做的事情少但关键：落库 URL、触发分片调度
   - 风险点: 偶发网络/DB 抖动, 所以用自动重试 + backoff + jitter来让它自己恢复, 在函数里做了幂等判断
   - 即使 webhook 重复、多 worker 同时收消息、任务自动重试，只会第一次写入 URL，后续都 “skipped”
   - 正常只触发一次，但如果webhook 发了两次（或手动又 delay() 了一次），那是两条任务消息，
   - 可能分别被两个 worker 执行, 所以需要幂等性
'''
def _handle_bulk_finish_logic(
    bulk_id: str, url: str, root_object_count: int | None = None
):
    db = SessionLocal()

    try:
         #  1. 根据bulkID 找到对应的 run 记录
        run = db.query(ProductSyncRun).filter_by(shopify_bulk_id=bulk_id).first()
        if not run:
            run = ProductSyncRun(shopify_bulk_id=bulk_id, status="running")
            db.add(run); db.commit(); db.refresh(run)

        # 2. 若重复调用（幂等）：已有 URL 就直接返回
        # if run.shopify_bulk_url:
        #     return {"run_id": run.id, "skipped": "url already saved"}
        
        # 3. 记录状态更新
        run.shopify_bulk_status = "COMPLETED"

        # 4. 更新 Bulk 状态/URL/数量
        run.shopify_bulk_url = url
        
        # Shopify 返回的是字符串数字，这里转 int, 失败默认 0
        if root_object_count is not None:
            try:
                run.total_shopify_skus = int(root_object_count or 0)
            except Exception:
                run.total_shopify_skus = 0

        db.commit()   # 提交了上述 3 个字段的更新

        # 5. 流式调度分片任务（5k/片）
        return schedule_chunks_streaming(run.id, url)

    finally:
        db.close()





'''
分片任务 5k/片 
   - 不自动重试, 子批 (≤50) 强重试在 DSZ 层
   - 入参 sku_codes 支持历史 List[str] 以及包含 {"sku", "variant_id"} 的结构化条目
'''
# 当前状态：用的是 Celery chord。如果任何一个分片任务抛异常，
# 默认 chord 的回调（finalize_run）不会被触发，整趟 run 会一直卡在 running。
# 也就是说——你现在没有“自动跳过”的能力
@shared_task(
    name="app.orchestration.product_sync.product_sync_task.process_chunk",
    # soft_time_limit=600,
)
def process_chunk(run_id: str, chunk_idx: int, 
    sku_codes: list[Any], use_counter: bool = False
):
    
    logger.info("process_chunk start run=%s idx=%s size=%s", run_id, chunk_idx, len(sku_codes))
    # === 全函数计时开始 ===
    _t_all_start = time.perf_counter()
    db = SessionLocal()
    # 这两个变量用于异常情况下也能打印
    _t_upsert_s = None
    _t_candidates_s = None
    stats: dict[str, Any] = {}
    changed_rows: list[dict] = []
    candidate_tuples: list[tuple[str, dict]] = []

    try:
        # Debug: 如果 sku_codes 中包含目标 SKU，则打印并记录日志，方便观测
        # target_sku = "FF-DINING-WD-BK"
        # def _extract_sku(entry: Any) -> Optional[str]:
        #     if isinstance(entry, str):
        #         return entry
        #     if isinstance(entry, dict):
        #         sku_val = entry.get("sku")
        #         if isinstance(sku_val, str):
        #             return sku_val
        #     return None

        # if any(_extract_sku(entry) == target_sku for entry in sku_codes):
        #     logger.info(
        #         "process_chunk run=%s idx=%s contains target SKU %s", run_id, chunk_idx, target_sku
        #     )
        #     print(f"process_chunk run={run_id} idx={chunk_idx} contains target SKU {target_sku}")


        _mark_chunk_running_safe(db, run_id, chunk_idx)
        
        # parse sku, price, variant id, product tags from shopify data
        skus, chunk_data_map = normalize_sku_payload(sku_codes)

        # 有些分片可能确实是空片, 把“空分片”视为成功: 没有要处理的数据 == 已经处理完
        if not skus:
            return _handle_empty_chunk(db, run_id, chunk_idx, _t_all_start)
        
        # 1) 从 DB 批量读取旧快照 & 变体ID映射
        old_map, vid_map = _prepare_existing_context(db, skus, chunk_data_map)

        # 2) 调 DSZ + 区域运费，产出标准化原始数据
        items, stats, zone_map = _fetch_remote_snapshots(skus)

        # 3) 根据dsz数据+shopify数据: 标准化 + 计算 attrs_hash_current
        normed = _normalize_snapshots(items, zone_map, vid_map, chunk_data_map)

        # 4) 依赖dsz返回数据: 逐 SKU 做 diff，收集需要 upsert 的行和候选字段
        changed_rows, candidate_tuples = _collect_chunk_changes(normed, old_map, run_id, chunk_idx)

        # 5) Upsert & 保存候选: changed_rows 和  changed_rows 字段可以完全对上
        _t_upsert_s, _t_candidates_s = _persist_chunk_changes(
            db, run_id, chunk_idx, changed_rows, candidate_tuples
        )

        # 6) 成功回写 manifest 指标并输出摘要
        _mark_chunk_success(db, run_id, chunk_idx, stats)
        db.commit()
        _log_chunk_summary(run_id, chunk_idx, stats)

        # === 总耗时打印 ===
        _t_all_s = time.perf_counter() - _t_all_start
        upsert_s = _t_upsert_s if _t_upsert_s is not None else 0.0
        candidates_s = _t_candidates_s if _t_candidates_s is not None else 0.0
        print(
            f"[TIMER] process_chunk run={run_id} idx={chunk_idx} total={_t_all_s:.3f} s "
            f"(upsert={upsert_s:.3f} s, candidates={candidates_s:.3f} s)",
            flush=True
        )

        return {
            "changed": len(changed_rows),
            "candidates": [sku for sku, _ in candidate_tuples],
            "missing_count": int(stats.get("missing_count", 0)),
            "extra_count": int(stats.get("extra_count", 0)),
            "failed_batches_count": int(stats.get("failed_batches_count", 0)),
            "failed_skus_count": int(stats.get("failed_skus_count", 0)),
            "requested_total": int(stats.get("requested_total", 0)),
            "returned_total": int(stats.get("returned_total", 0)),
        }
    
    except Exception as e:
        db.rollback()
        _mark_chunk_failed_safe(db, run_id, chunk_idx, e)
        logger.exception("chunk failed: run=%s idx=%s err=%s", run_id, chunk_idx, e)

        # === 总耗时打印（异常路径）===
        _t_all_s = time.perf_counter() - _t_all_start
        upsert_s = _t_upsert_s if _t_upsert_s is not None else 0.0
        candidates_s = _t_candidates_s if _t_candidates_s is not None else 0.0
        print(
            f"[TIMER] process_chunk run={run_id} idx={chunk_idx} total={_t_all_s:.3f} s (FAILED) "
            f"(upsert={upsert_s:.3f} s, candidates={candidates_s:.3f} s)",
            flush=True
        )
        
        return {
            "changed": 0, "candidates": [], "missing_count": 0, "extra_count": 0,
            "failed_batches_count": 0, "failed_skus_count": 0, "requested_total": 0, "returned_total": 0,
        }
    
    finally:
        db.close()
        logger.info("process_chunk end idx=%s size=%s", chunk_idx, len(sku_codes))




# ===== 汇总：兼容 manifest 汇总，标 completed/completed_with_gaps，再触发运费 =====
@shared_task(name="app.orchestration.product_sync.product_sync_task.finalize_run")
def finalize_run(results, run_id: str):
    # 先用传参（来自 chord）的结果做一次快速聚合
    total_changed = 0
    sku_set: set[str] = set()
    missing_sum = 0
    failed_batches = 0
    failed_skus = 0
    # 汇总 DSZ 请求/返回规模，用于计算缺失占比
    requested_sum = 0
    returned_sum = 0

    for r in results or []:
        total_changed += int((r or {}).get("changed", 0))
        sku_set.update((r or {}).get("candidates") or [])
        missing_sum += int((r or {}).get("missing_count", 0))
        failed_batches += int((r or {}).get("failed_batches_count", 0))
        failed_skus    += int((r or {}).get("failed_skus_count", 0))
        # 新增两项的聚合
        requested_sum  += int((r or {}).get("requested_total", 0))
        returned_sum   += int((r or {}).get("returned_total", 0))

    # 再从 manifest 补齐健康度与缺口（避免结果不全时误判）
    db = SessionLocal()
    failed_chunks = 0
    pending_chunks = 0
    purge_needed = False
    try:
        rows = (
            db.query(ProductSyncChunk.status,
                     func.sum(ProductSyncChunk.dsz_missing),
                     func.sum(ProductSyncChunk.dsz_failed_batches),
                     func.sum(ProductSyncChunk.dsz_failed_skus),
                     func.sum(ProductSyncChunk.dsz_requested_total),
                     func.sum(ProductSyncChunk.dsz_returned_total))
            .filter(ProductSyncChunk.run_id == run_id)
            .group_by(ProductSyncChunk.status)
            .all()
        )
        for st, m_sum, fb_sum, fs_sum, rq_sum, rt_sum in rows:
            if st == "succeeded":
                missing_sum       += int(m_sum or 0)
                failed_batches    += int(fb_sum or 0)
                failed_skus       += int(fs_sum or 0)
                requested_sum     += int(rq_sum or 0)
                returned_sum      += int(rt_sum or 0)
            elif st == "failed":
                failed_chunks += 1
            elif st in ("pending", "running"):
                pending_chunks += 1
    finally:
        db.close()
    
    # 是否需要清理缺失 SKU（仅在无失败、无待处理时触发）
    purge_needed = (failed_chunks == 0 and pending_chunks == 0)

    # 阈值健康度检查与告警（放在落库前后都可，这里先做）
    _maybe_alert_dsz_health(
        run_id=str(run_id),
        missing_sum=missing_sum,
        failed_batches=failed_batches,
        failed_skus=failed_skus,
        requested_sum=requested_sum,
    )

    # 标记 run 状态
    db = SessionLocal()
    try:
        run = db.get(ProductSyncRun, run_id)
        if run:
            # 有缺口则标记 completed_with_gaps，否则 completed
            run.status = "completed_with_gaps" if (failed_chunks or pending_chunks) else "completed"
            run.finished_at = datetime.now(timezone.utc)
            run.changed_count = total_changed

            # 可选：把汇总指标也落库，便于后续可视化/审计（字段存在才赋值）
            try:
                if hasattr(run, "requested_total_sum"): run.requested_total_sum = requested_sum
                if hasattr(run, "returned_total_sum"): run.returned_total_sum = returned_sum
                if hasattr(run, "missing_total_sum"): run.missing_total_sum = missing_sum
                if hasattr(run, "failed_batches_sum"): run.failed_batches_sum = failed_batches
                if hasattr(run, "failed_skus_sum"): run.failed_skus_sum = failed_skus
            except Exception:
                pass

            db.commit()
    finally:
        db.close()

    # 清理 Shopify 已不存在的 SKU 信息
    # if purge_needed:
    #     try:
    #         _purge_absent_skus(run_id)
    #     except Exception:
    #         logger.exception("failed to purge absent skus: run=%s", run_id)

    logger.info(
        "final summary: run=%s candidates=%d missing=%d failed_chunks=%d pending_chunks=%d",
        run_id, len(sku_set), missing_sum, failed_chunks, pending_chunks
    )

    # 触发运费计算（优先候选）:是在 finalize_run（chord 回调）里触发一次，而不是在每个 process_chunk 里触发。
    # 所有切片 → 1 次 finalize_run → 1 次 触发运费
    try:
        if _inline_tasks_enabled():
            kick_freight_calc.run(
                product_run_id=str(run_id), trigger="product-sync-trigger"
            )
        else:
            kick_freight_calc.delay(
                product_run_id=str(run_id), trigger="product-sync-trigger"
            )
    except Exception:
        pass

    return {"run_id": run_id, "candidate_skus": len(sku_set)}



def _resume_running_run_if_any(
    *, inline: bool, poll_kwargs: Optional[Dict[str, Any]]
) -> Optional[str]:
    db = SessionLocal()
    try:
        row = (
            db.query(ProductSyncRun)
            .filter(ProductSyncRun.status == "running")
            .order_by(ProductSyncRun.started_at.asc())
            .first()
        )
        if not row:
            return None

        run_id = str(row.id)
        bulk_id = row.shopify_bulk_id
        bulk_url = row.shopify_bulk_url
        total_skus = row.total_shopify_skus or 0
    finally:
        db.close()

    resumed = _resume_existing_run(
        run_id=run_id,
        bulk_id=bulk_id,
        bulk_url=bulk_url,
        total_skus=total_skus,
        inline=inline,
        poll_kwargs=poll_kwargs,
    )
    return run_id if resumed else None


def _resume_existing_run(
    *,
    run_id: str,
    bulk_id: Optional[str],
    bulk_url: Optional[str],
    total_skus: int,
    inline: bool,
    poll_kwargs: Optional[Dict[str, Any]],
) -> bool:
    
    # 1- 只有shopify_bulk_id 没有 URL：重新启动 bulk 轮询
    if bulk_id and not bulk_url:
        logger.info("resume run=%s by restarting bulk polling", run_id)
        if inline:
            poll_kwargs = poll_kwargs or {}
            poll_bulk_until_ready_inline(run_id, **(poll_kwargs or {}))
        else:
            task_id = f"poll:{bulk_id}"
            poll_bulk_until_ready.apply_async(args=[run_id], countdown=0, task_id=task_id)
        return True

    if not bulk_url:
        return False
    
    
    status_counts, manifest_total = _get_chunk_status_counts(run_id)
    expected_chunks = _expected_chunk_count(total_skus)
    # 2 - case 判断：
    # manifest_total == 0：说明 manifest 里一个 chunk 都没有，通常是还没切片或切片阶段就崩溃，这时需要重新跑 schedule_chunks_streaming。
    # manifest_total < expected_chunks：说明根据 total_skus 估算应该有更多 chunk，但 manifest 当前数量不足，可能是切片只完成一部分就中断，也应该重新切一次把缺失的 chunk 补齐。
    need_rechunk = manifest_total == 0 or (
        expected_chunks is not None and manifest_total < expected_chunks
    )

    # 3 - 已有 URL 但 manifest 不完整（为空或数量少于应该的 chunk 数）：重新调用 schedule_chunks_streaming，从 Shopify URL 再切一遍，把缺失 chunk 重建出来
    if need_rechunk:
        logger.info("resume run=%s by rebuilding manifest via streaming", run_id)
        schedule_chunks_streaming(run_id, bulk_url)
        return True
    
    # 4 - manifest 已存在但有 pending/running/failed chunk：调用 schedule_chunks_from_manifest 只重投这些 chunk；
    pending_statuses = tuple(
        st for st in ("pending", "running", "failed") if status_counts.get(st)
    )
    if pending_statuses:
        logger.info("resume run=%s by rescheduling manifest chunks statuses=%s", run_id, pending_statuses,)
        schedule_chunks_from_manifest(run_id, statuses=pending_statuses)
        return True
    
    # 5 - 所有分片都 succeeded，但 run 仍未完成，触发 finalize。
    logger.info("resume run=%s: manifest done, triggering finalize", run_id)
    if inline or _inline_tasks_enabled():
        finalize_run.run([], run_id)
    else:
        finalize_run.delay([], run_id)
    return True



# 查 product_sync_chunks 表里属于该 run_id 的所有分片
#    - 按 status 统计数量。返回值是两个部分：counts（如 {"pending":2,"succeeded":8}）
#    - 以及 manifest_total（各状态数量之和）。
#    - 这个函数用来了解 manifest 当前的整体进度，有没有 chunk 还没跑、有没有失败等。
def _get_chunk_status_counts(run_id: str) -> tuple[Dict[str, int], int]:
    db = SessionLocal()
    try:
        rows = (
            db.query(ProductSyncChunk.status, func.count())
            .filter(ProductSyncChunk.run_id == run_id)
            .group_by(ProductSyncChunk.status)
            .all()
        )
    finally:
        db.close()
    counts = {st: int(cnt) for st, cnt in rows}
    total = sum(counts.values())
    return counts, total


# 根据当天 Shopify Bulk 预计的总 SKU 数，推算“应该有多少个 chunk”
#    - 计算方式是：total_skus / SYNC_CHUNK_SKUS 向上取整。
#    - eg: 比如总共 23,000 个 SKU，默认每片 5,000，则期望是 5 个 chunk
#    - 如果 total_skus 未知（0 或 None），就返回 None，表示没法判断。
def _expected_chunk_count(total_skus: int) -> Optional[int]:
    if not total_skus:
        return None
    size = max(1, int(SYNC_CHUNK_SKUS or 5000))
    return math.ceil(total_skus / size)




"""
    按当前 run 的 manifest 删除 Shopify 已不存在的历史 SKU。
    仅在 full sync 成功收尾后调用。
"""
def _purge_absent_skus(run_id: str) -> None:
    db = SessionLocal()
    removed: List[str] = []
    try:
        keep_skus = collect_shopify_skus_for_run(db, run_id)
        if not keep_skus:
            logger.warning("skip purge: run=%s has no manifest skus", run_id)
            return

        removed = purge_sku_info_absent_from(db, keep_skus)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    if removed:
        sample = removed[:10]
        logger.info(
            "purged %d sku_info rows absent from Shopify for run=%s (sample=%s)",
            len(removed), run_id, sample,
        )
    else:
        logger.info("sku_info purge found nothing to delete for run=%s", run_id)



'''
周期性兜底：为“running && 有 bulk_id 但没 url”的 run 再触发轮询
   - 使用场景: 兜底轮询shopify依旧失败之后
   - 使用方式: url 运维接口手动触发
'''
# @shared_task(name="app.orchestration.product_sync.product_sync_task.bulk_url_sweeper")
def bulk_url_sweeper():
    db = SessionLocal()
    try:
        q = db.query(ProductSyncRun).filter(
            ProductSyncRun.status == "running",
            ProductSyncRun.shopify_bulk_id.isnot(None),
            ProductSyncRun.shopify_bulk_url.is_(None),
        ).order_by(ProductSyncRun.started_at.asc()).limit(20)

        ids = [r.id for r in q.all()]
        for rid in ids:
            run = db.get(ProductSyncRun, rid)
            if not run or not run.shopify_bulk_id:
                continue

            task_id = f"poll:{run.shopify_bulk_id}" 
            # 同一 bulk 使用同一 task_id，避免多 poller
            if _inline_tasks_enabled():
                poll_bulk_until_ready_inline(rid)
            else:
                poll_bulk_until_ready.apply_async(
                    args=[rid],
                    task_id=task_id,
                    countdown=40,    # 40s 后开始兜底轮询
                )

        return {"scheduled": len(ids)}
    finally:
        db.close()





# ==================== helper functions =================
def _is_sqlalchemy_expression(value: Any) -> bool:
    """
    Detect SQLAlchemy expressions / ORM attributes that should not appear in payloads.
    """
    if isinstance(value, (ClauseElement, BindParameter, QueryableAttribute)):
        return True
    if hasattr(value, "__clause_element__"):
        return True
    return False


def _assert_plain_snapshot_values(snapshot: Dict[str, Any], run_id: str, chunk_idx: int) -> None:
    """
    Guard against accidental SQLAlchemy expressions leaking into persistence payloads.
    """
    sku = snapshot.get("sku_code")
    for field, value in snapshot.items():
        if _is_sqlalchemy_expression(value):
            logger.error(
                "process_chunk detected non-plain value: run=%s idx=%s sku=%s field=%s type=%s value=%r",
                run_id,
                chunk_idx,
                sku,
                field,
                type(value),
                value,
            )
            raise ValueError(
                f"process_chunk detected SQL expression in snapshot: run={run_id} chunk={chunk_idx} sku={sku} field={field} type={type(value)!r}"
            )


def _send_ops_alert(message: str) -> None:
    """
    优先用 Slack Webhook 发送；如果未配置，则退化为 ERROR 日志。
    """
    webhook = getattr(settings, "SLACK_WEBHOOK_URL", os.getenv("SLACK_WEBHOOK_URL"))
    if webhook:
        try:
            requests.post(webhook, json={"text": message}, timeout=5)
        except Exception:
            logger.exception("send slack webhook failed")
    logger.error(message)


def _maybe_alert_dsz_health(*, run_id: str, missing_sum: int, failed_batches: int, failed_skus: int,
                            requested_sum: int) -> None:
    """
    依据阈值对 DSZ 健康度进行告警：
    - 缺失占比 > DSZ_MISSING_RATIO_ALERT
    - 失败子批 > DSZ_FAILED_BATCHES_ALERT
    - 失败SKU  > DSZ_FAILED_SKUS_ALERT
    """
    # 阈值（可通过 settings 配置）
    missing_ratio_threshold = float(getattr(settings, "DSZ_MISSING_RATIO_ALERT", 0.02))  # 2%
    failed_batches_threshold = int(getattr(settings, "DSZ_FAILED_BATCHES_ALERT", 0))
    failed_skus_threshold = int(getattr(settings, "DSZ_FAILED_SKUS_ALERT", 0))

    requested_sum = max(1, int(requested_sum or 0))  # 防除零
    missing_ratio = missing_sum / requested_sum

    alerts = []
    if missing_ratio > missing_ratio_threshold:
        alerts.append(f"missing_ratio={missing_ratio:.2%} > {missing_ratio_threshold:.2%}")
    if failed_batches > failed_batches_threshold:
        alerts.append(f"failed_batches={failed_batches} > {failed_batches_threshold}")
    if failed_skus > failed_skus_threshold:
        alerts.append(f"failed_skus={failed_skus} > {failed_skus_threshold}")

    if alerts:
        _send_ops_alert(
            f"[DSZ-HEALTH ALERT] run={run_id} | "
            f"{'; '.join(alerts)} | "
            f"requested_sum={requested_sum}, missing_sum={missing_sum}, "
            f"failed_batches={failed_batches}, failed_skus={failed_skus}"
        )



# === chunk helpers ===
def _mark_chunk_running_safe(db: Session, run_id: str, chunk_idx: int) -> None:
    try:
        mark_chunk_running(db, run_id, chunk_idx)
        db.commit()
    except Exception as exc:
        db.rollback()
        raise exc


def _handle_empty_chunk(db, run_id: str, chunk_idx: int, start_ts: float) -> dict:
    stats = {
        "missing_count": 0,
        "failed_batches_count": 0,
        "failed_skus_count": 0,
        "requested_total": 0,
        "returned_total": 0,
        "extra_count": 0,
        "missing_sku_list": [],
        "failed_sku_list": [],
        "extra_sku_list": [],
    }
    _mark_chunk_success(db, run_id, chunk_idx, stats)
    db.commit()
    _log_chunk_summary(run_id, chunk_idx, stats)
    total = time.perf_counter() - start_ts
    print(
        f"[TIMER] process_chunk run={run_id} idx={chunk_idx} total={total:.3f} s (empty chunk)",
        flush=True,
    )
    return {
        "changed": 0,
        "candidates": [],
        "missing_count": 0,
        "extra_count": 0,
        "failed_batches_count": 0,
        "failed_skus_count": 0,
        "requested_total": 0,
        "returned_total": 0,
    }


def _prepare_existing_context(db, skus: list[str], chunk_data_map: dict[str, dict]) -> tuple[dict, dict]:
    old_map = load_existing_by_skus(db, skus)
    vid_map = load_variant_ids_by_skus(db, skus)
    for sku, payload in chunk_data_map.items():
        variant_id = payload.get("shopify_variant_id")
        if variant_id:
            vid_map[sku] = variant_id
    return old_map, vid_map


def _fetch_remote_snapshots(skus: list[str]) -> tuple[list[dict], dict[str, Any], dict[str, dict]]:
    items, stats = get_products_by_skus_with_stats(skus)
    zone_map = _build_zone_map(get_zone_rates_by_skus(skus))
    stats = stats or {}
    _trim_stats_lists(stats)
    return items, stats, zone_map


def _build_zone_map(zone_list: list[dict] | None) -> dict[str, dict]:
    zone_map: dict[str, dict] = {}
    for z in zone_list or []:
        if not isinstance(z, dict):
            continue
        sku = str(z.get("sku") or "").strip()
        std = z.get("standard")
        if sku and isinstance(std, dict):
            zone_map[sku] = std
    return zone_map


def _trim_stats_lists(stats: dict[str, Any]) -> None:
    max_list = int(getattr(settings, "DSZ_DETAIL_LIST_MAX_PER_CHUNK", 500))
    for key in ("missing_sku_list", "failed_sku_list", "extra_sku_list"):
        value = stats.get(key)
        if isinstance(value, list):
            stats[key] = value[:max_list]
        else:
            stats[key] = []


def _normalize_snapshots(
    items: list[dict],
    zone_map: dict[str, dict],
    vid_map: dict[str, Any],
    chunk_data_map: dict[str, dict],
) -> list[dict]:
    normed: list[dict] = []
    # 遍历dsz items
    for raw in items:
        sku_raw = str((raw or {}).get("sku") or "").strip()
        std = zone_map.get(sku_raw)
        raw_for_norm = dict(raw) if isinstance(raw, dict) else {}
        if std:
            raw_for_norm["_zone_standard"] = std

        n = normalize_dsz_product(raw_for_norm) # 31个
        sku = n.get("sku_code")
        if sku:
            enrich_shopify_snapshot(n, sku, vid_map, chunk_data_map) # 3个
        
        # 计算hashvalue
        n["attrs_hash_current"] = calc_attrs_hash_current(n) # 1个
        normed.append(n)
    return normed


def _collect_chunk_changes(
    normed: list[dict],  # 本次dsz+shopify新数据
    old_map: dict[str, dict],  #对比的历史数据
    run_id: str,
    chunk_idx: int,
) -> tuple[list[dict], list[tuple[str, dict]]]:
    
    # 构建待更新list： 用于后续 bulk_upsert_sku_info, 用于 save_candidates
    changed_rows: list[dict] = []    
    candidate_tuples: list[tuple[str, dict]] = []

    for snapshot in normed:
        sku = snapshot["sku_code"]
        # 防御意外的 SQL 表达式混入 payload（确保都是普通值）。
        _assert_plain_snapshot_values(snapshot, run_id, chunk_idx)

        # 旧快照（可能为 None）
        old = old_map.get(sku)

        # 有变化则把整行 snapshot 追加到 changed_rows
        changed_fields = diff_snapshot(old, snapshot)
        if not changed_fields:
            continue
        changed_rows.append(snapshot)

        # 基于 changed_fields 构建仅包含“变更字段”的 new_partial: changed_fields 只包含发生变化的列名，最终 new_partial 当然只含这些列的键值对
        new_partial = {k: snapshot.get(k) for k in changed_fields}
        if "attrs_hash_current" not in new_partial and "attrs_hash_current" in snapshot:
            new_partial["attrs_hash_current"] = snapshot.get("attrs_hash_current")
        candidate_tuples.append((sku, new_partial))

    return changed_rows, candidate_tuples




def _persist_chunk_changes(
    db,
    run_id: str,
    chunk_idx: int,
    changed_rows: list[dict],   # 一行完整的新数据sku_info
    candidate_tuples: list[tuple[str, dict]],
) -> tuple[float, float]:
    if not changed_rows:
        return 0.0, 0.0
    candidate_rows: list[dict] = []
    try:

        # 批量更新/写入sku_info
        t0 = time.perf_counter()
        bulk_upsert_sku_info(db, changed_rows, only_update_when_changed=True)
        upsert_elapsed = time.perf_counter() - t0
        print(f"[TIMER] process_chunk run={run_id} idx={chunk_idx} bulk_upsert_sku_info rows={len(changed_rows)} time={upsert_elapsed:.3f} s",flush=True,)

        # 批量写入 product_sync_candidates
        candidates_elapsed = 0.0
        candidate_rows = build_candidate_rows(run_id, candidate_tuples)
        if candidate_rows:
            t1 = time.perf_counter()
            save_candidates(db, candidate_rows)
            candidates_elapsed = time.perf_counter() - t1
            print(f"[TIMER] process_chunk run={run_id} idx={chunk_idx} save_candidates rows={len(candidate_rows)} time={candidates_elapsed:.3f} s",flush=True,)

        return upsert_elapsed, candidates_elapsed
    except Exception:
        logger.exception(
            "failed to persist chunk changes: run=%s idx=%s rows=%d candidates=%d",
            run_id,
            chunk_idx,
            len(changed_rows),
            len(candidate_rows),
        )
        raise


def _mark_chunk_success(db, run_id: str, chunk_idx: int, stats: dict[str, Any]) -> None:
    try:
        mark_chunk_succeeded(db, run_id, chunk_idx, stats)
    except Exception as exc:
        db.rollback()
        raise exc


def _mark_chunk_failed_safe(db: Session, run_id: str, chunk_idx: int, err: Exception | str) -> None:
    try:
        mark_chunk_failed(db, run_id, chunk_idx, err)
        db.commit()
    except Exception:
        db.rollback()


def _log_chunk_summary(run_id: str, chunk_idx: int, stats: dict[str, Any]) -> None:
    stats = stats or {}
    logger.info(
        "chunk summary: run=%s idx=%s requested=%d returned=%d missing=%d missing_list=%d "
        "failed_batches=%d failed_skus=%d failed_sku_list=%d extra=%d extra_list=%d",
        run_id,
        chunk_idx,
        int(stats.get("requested_total", 0) or 0),
        int(stats.get("returned_total", 0) or 0),
        int(stats.get("missing_count", 0) or 0),
        len(stats.get("missing_sku_list", []) or []),
        int(stats.get("failed_batches_count", 0) or 0),
        int(stats.get("failed_skus_count", 0) or 0),
        len(stats.get("failed_sku_list", []) or []),
        int(stats.get("extra_count", 0) or 0),
        len(stats.get("extra_sku_list", []) or []),
    )


# [NEW] —— Redis 工具（可选，没装/没配会自动跳过）
def _redis():
    url = getattr(settings, "REDIS_URL", None)
    if not (redis and url):
        return None
    try:
        return redis.from_url(url, decode_responses=True)
    except Exception:
        return None
