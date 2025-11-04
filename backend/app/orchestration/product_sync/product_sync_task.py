
from __future__ import annotations
import logging, json, time
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Callable
import os
import requests

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
    load_existing_by_skus, diff_snapshot, bulk_upsert_sku_info, save_candidates,
    load_variant_ids_by_skus, mark_chunk_running, mark_chunk_succeeded, mark_chunk_failed,
    collect_shopify_skus_for_run, purge_sku_info_absent_from,
)
from app.orchestration.product_sync.scheduler import schedule_chunks_streaming
from app.orchestration.product_sync.chunk_enricher import enrich_shopify_snapshot
from app.orchestration.freight_calculation.freight_task import kick_freight_calc

# 可选 Redis 锁
try:
    import redis  # redis-py
except Exception:
    redis = None


logger = logging.getLogger(__name__)
_shopify = ShopifyClient()  # 单实例即可



"""
  调试开关：True 时所有子任务在当前进程内同步执行。
"""
def _inline_tasks_enabled() -> bool:
    return True




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
    TAG = getattr(settings, "SHOPIFY_TAG_FULL_SYNC", "DropshipzoneAU")
    db = SessionLocal()

    try:
        # 1. 建立 run 记录
        run = ProductSyncRun(status="running", run_type="full_sync")
        db.add(run); db.commit(); db.refresh(run)

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
        
        # 3. 保存 bulk_id 并安排轮询
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




@shared_task(
    name="app.tasks.product_full_sync.poll_bulk_until_ready",
    bind=True, max_retries= 40, default_retry_delay= 60
    # 最多 40 次，每次间隔 60s → 最长约 40 分钟（指数退避）实际测试 3min
)
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
            return (False, None, result or "handled via polling")

        return (True, _poll_retry_delay(attempt, default_retry_delay), None)

    finally:
        db.close()




"""
    根据执行模式触发 handle_bulk_finish。
"""
def _dispatch_handle_bulk_finish(
    bulk_id: str, url: str, root_object_count: Any | None, *, inline: bool
):
    if inline or _inline_tasks_enabled():
        return handle_bulk_finish_inline(bulk_id, url, root_object_count)

    handle_bulk_finish.apply_async(
        args=[bulk_id, url, root_object_count], task_id=f"finish:{bulk_id}"
    )
    return "handle_bulk_finish dispatched"



"""
   与 Celery 任务保持一致的指数退避延迟计算。
"""
def _poll_retry_delay(attempt: int, default_retry_delay: int = 60) -> int:
    delay = int(default_retry_delay * (1.2 ** max(0, attempt)))
    return min(60, max(1, delay))



@shared_task(
    name="app.orchestration.product_sync_task.handle_bulk_finish",
    bind=True,
    max_retries=5,
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

    db = SessionLocal()
    try:
        try:
            mark_chunk_running(db, run_id, chunk_idx)    # 标记 manifest：product_sync_chunks running
            db.commit()
        except Exception:
            db.rollback()
        
        # parse sku, price, variant id, product tags
        skus, chunk_data_map = normalize_sku_payload(sku_codes)

        # 有些分片可能确实是空片, 把“空分片”视为成功: 没有要处理的数据 == 已经处理完
        if not skus:
            mark_chunk_succeeded(db, run_id, chunk_idx, stats={
                "missing_count":0,"failed_batches_count":0,"failed_skus_count":0,
                "requested_total":0,"returned_total":0,
                "missing_sku_list": [], "failed_sku_list": [], "extra_sku_list": [] 
            })  
            db.commit()
            return {
                "changed": 0, "candidates": [], "missing_count": 0, "extra_count": 0,
                "failed_batches_count": 0, "failed_skus_count": 0, "requested_total": 0, 
                "returned_total": 0,
            }
        
        # 1) 从 DB 批量读取旧快照 & 变体ID映射
        old_map = load_existing_by_skus(db, skus)
        vid_map = load_variant_ids_by_skus(db, skus)
        for sku, payload in chunk_data_map.items():
            variant_id = payload.get("shopify_variant_id")
            if variant_id:
                vid_map[sku] = variant_id

        # 2) 调 DSZ（内部：≤50/批 + 强重试 + 一致性告警）
        # todo 让 get_products_by_skus_with_stats 把失败的 sku 列表（无法返回/子批失败）交出来？
        items, stats = get_products_by_skus_with_stats(skus)

        # 3) 增加查询dsz的新接口获取sku的运费数据 /v2/get_zone_rates（仅 sku + standard）
        zone_list = get_zone_rates_by_skus(skus)
        zone_map: dict[str, dict] = {}
        for z in zone_list or []:
            if isinstance(z, dict):
                s = str(z.get("sku") or "").strip()
                std = z.get("standard")
                if s and isinstance(std, dict):
                    zone_map[s] = std

        
        # 明细限长，防止单片过大（默认 500；可用 settings 配置）
        MAX_LIST = int(getattr(settings, "DSZ_DETAIL_LIST_MAX_PER_CHUNK", 500))  
        for key in ("missing_sku_list", "failed_sku_list", "extra_sku_list"): 
            if key in stats and isinstance(stats[key], list):
                stats[key] = stats[key][:MAX_LIST]         
            else:
                stats[key] = []   

        # 3) 把 DSZ 原始结构转换成系统内部统一的数据结构 + 拼接 attrsHash + 变体映射 
        normed: list[dict] = []
        for raw in items:
            sku_raw = str((raw or {}).get("sku") or "").strip()  
            std = zone_map.get(sku_raw)  
            raw_for_norm = dict(raw) if isinstance(raw, dict) else {} 
            if std:  # 注入给 normalizer
                raw_for_norm["_zone_standard"] = std
                

            # dsz + shopify 归一化内部字段
            n = normalize_dsz_product(raw_for_norm)

            # 补充 shopify 侧的增量字段
            sku = n.get("sku_code")
            if sku:
                enrich_shopify_snapshot(n, sku, vid_map, chunk_data_map)

            # 计算属性哈希 —— 只使用 FREIGHT_HASH_FIELDS 中的入参字段
            # 这是“运费敏感字段哈希”的唯一落库点：存入 SkuInfo.attrs_hash_current
            n["attrs_hash_current"] = calc_attrs_hash_current(n)
            # if sku:
            #     print(f"product candidate sku={sku} length={n.get('length')!r} width={n.get('width')!r} height={n.get('height')!r}")
            normed.append(n)


        # 4) 逐 SKU 做 diff，收集“需要更新”的变更
        changed_rows: list[dict] = []                   # 真正需要 upsert 到 DB 的完整记录集合
        candidate_tuples: list[tuple[str, dict]] = []   # 作为“候选变更”的轻量集合，用来喂给候选表，供后续流程（例如运费计算
        
        for n in normed:
            sku = n["sku_code"]
            old = old_map.get(sku)      # 找到旧记录

            changed_fields = diff_snapshot(old, n)   # 算差异: 通常会返回变化字段名集合或 {字段名: 新值} 的子集, 新增：也会被视为“有差异”, 无变化：返回空/None。
            if changed_fields:
                changed_rows.append(n)                               # 把完整新记录加入 upsert 列表
                new_partial = {k: n.get(k) for k in changed_fields}  # 提炼出变更字段的子集

                # 记录成 (sku, 变更字段子集)，写入候选池
                candidate_tuples.append((sku, new_partial)) 


        # 5) upsert & 保存候选 一次事务提交，出错回滚
        if changed_rows or candidate_tuples:
            try:
                if changed_rows:
                    bulk_upsert_sku_info(db, changed_rows, only_update_when_changed=True)
                if candidate_tuples:
                    save_candidates(db, run_id, candidate_tuples)
                db.commit()
            except Exception as exc:
                db.rollback()
                logger.exception(
                    "failed to persist chunk changes: run=%s idx=%s rows=%d candidates=%d",
                    run_id,
                    chunk_idx,
                    len(changed_rows),
                    len(candidate_tuples),
                )
                raise

        # 6) 成功回写 manifest 指标
        try:
            mark_chunk_succeeded(db, run_id, chunk_idx, stats)
            db.commit()
        except Exception:
            db.rollback()

        logger.info(
            "chunk summary: run=%s idx=%s requested=%d returned=%d missing=%d failed_batches=%d failed_skus=%d",
            run_id, chunk_idx, stats.get("requested_total"), stats.get("returned_total"),
            stats.get("missing_count"), len(stats.get("missing_sku_list", [])),
            stats.get("failed_batches_count", 0), stats.get("failed_skus_count", 0),
            len(stats.get("failed_sku_list", [])), 
            stats.get("extra_count", 0), len(stats.get("extra_sku_list", [])), # todo?
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
        # 失败：更新 manifest 为 failed，但不抛出，让 chord 能继续
        try:
            mark_chunk_failed(db, run_id, chunk_idx, e) 
            db.commit()
        except Exception:
            db.rollback()

        logger.exception("chunk failed: run=%s idx=%s err=%s", run_id, chunk_idx, e)
        return {
            "changed": 0, "candidates": [], "missing_count": 0, "extra_count": 0,
            "failed_batches_count": 0, "failed_skus_count": 0, "requested_total": 0, "returned_total": 0,
        }
    
    finally:
        db.close()



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
    # try:
    #     if _inline_tasks_enabled():
    #         kick_freight_calc.run(
    #             product_run_id=str(run_id), trigger="product-sync-trigger"
    #         )
    #     else:
    #         kick_freight_calc.delay(
    #             product_run_id=str(run_id), trigger="product-sync-trigger"
    #         )
    # except Exception:
    #     pass

    return {"run_id": run_id, "candidate_skus": len(sku_set)}




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



# [NEW] —— Redis 工具（可选，没装/没配会自动跳过）
def _redis():
    url = getattr(settings, "REDIS_URL", None)
    if not (redis and url):
        return None
    try:
        return redis.from_url(url, decode_responses=True)
    except Exception:
        return None
