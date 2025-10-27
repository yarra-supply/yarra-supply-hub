
from __future__ import annotations
import json
import logging
from typing import Iterable, Iterator, List, Tuple, Dict, Any, Optional
import requests

from sqlalchemy import func  # 用于 upsert 时的时间戳（如果仍在本文件用）
from celery import chord, group, shared_task
from celery.canvas import signature
from app.db.session import SessionLocal
from app.db.model.product import ProductSyncChunk 
from app.repository.product_repo import upsert_chunk_pending

from app.core.config import settings


logger = logging.getLogger(__name__)

# 测试用
def _inline_tasks_enabled() -> bool:
    return True
    # return bool(getattr(settings, "SYNC_TASKS_INLINE", True))

SYNC_CHUNK_SKUS: int = getattr(settings, "SYNC_CHUNK_SKUS", 4000)  # 默认 5k/片
CHORD_SPLIT_AT: int = getattr(settings, "chord_split_at", 200)     # 单个 chord 的最大 header 数量，超出则分层



# ====== 流式切片调度（5k/片）→ chord 汇总 ========
# 边读 Bulk 结果，攒够 chunk_size（默认5k）就发一个 process_chunk 任务
#    - 最后用 chord 等所有分片完成后触发 finalize_run。
#    - 允许本任务自己重试（注意：只是 handle_bulk_finish，不是 5k 分片）
#    - 分片任务不自动重试；子批（≤50）强重试在 DSZ 层, 汇总任务不自动重试
#    - 返回 Celery AsyncResult id（便于追踪）。
#    - 每一个分片都会在 DB 里有一条 manifest 记录，可据此做按编号重投、观察状态与质量
# 好处：省内存（不保存 4 万行在内存），更快开跑（读到第 5000 行就可以发第一片，不用等全量下完）
def schedule_chunks_streaming(
        run_id: str, url: str, chunk_size: int = SYNC_CHUNK_SKUS, 
        # 通过任务名解耦，不再 import 任务函数
        process_task_name: str = "app.orchestration.product_sync.product_sync_task.process_chunk",
        finalize_task_name: str = "app.orchestration.product_sync.product_sync_task.finalize_run",
):

    buf_pairs: List[Tuple[str, str]] = []  # (sku, variant_id)
    sigs = []
    

    #====测试用 =====#
    inline_mode = _inline_tasks_enabled()
    inline_results: List[dict] = [] if inline_mode else None
    head_limit = 20
    #====测试用 =====#


    idx = 0               # 分片序号（用于确定性 task_id）
    db = SessionLocal()  

    try: 
        # 在缓冲 pairs 里已经攒够一批 (sku, variant_id) 时才会被调用
        def flush_chunk(pairs: List[Tuple[str, str]], chunk_idx: int):
            nonlocal sigs, inline_results
            if not pairs:
                return
            sku_codes = [s for s, _ in pairs]
            task_id = f"ps:chunk:{run_id}:{chunk_idx}"

            # sql更新/写入，此时未commit，不会写入
            upsert_chunk_pending(db, run_id, chunk_idx, sku_codes)
            db.commit()  # 立即提交 manifest，保证后续任务可见

            if inline_mode:
                from app.orchestration.product_sync.product_sync_task import process_chunk
                inline_results.append(process_chunk.run(run_id, chunk_idx, sku_codes, False))
            else:
                sig = signature(process_task_name).s(run_id, chunk_idx, sku_codes, False).set(task_id=task_id)
                sigs.append(sig)

        # todo check 流式从url下载数据 
        # test
        source_iter = iter_variant_from_bulk_head(url, limit=head_limit)
        for pair in source_iter:
        # for pair in iter_variant_from_bulk(url):
            buf_pairs.append(pair)
            if len(buf_pairs) >= chunk_size:
                # 在缓冲 pairs 里已经攒够一批 (sku, variant_id) 时才会被调用
                flush_chunk(buf_pairs, idx)
                buf_pairs.clear()
                idx += 1

        if buf_pairs:
            flush_chunk(buf_pairs, idx)
            buf_pairs.clear()
            idx += 1

        # 此时 ProductSyncChunk 才会写入 
        # todo 还需要吗？现在只是个安全收尾
        db.commit()

    finally:
        db.close()

    if inline_mode:
        from app.orchestration.product_sync.product_sync_task import finalize_run
        results = inline_results or []
        return finalize_run.run(results, run_id)

    if not sigs:
        signature(finalize_task_name).delay([], run_id)   # 没有SKU也要收口
        return {"run_id": run_id, "chunks": 0}

    # 若“单个巨大 chord”不稳定/易失败，则把 header 拆分为多个“小 chord”，
    # 每个小 chord 的回调先聚合子结果；再用一个“外层 chord”统一收口并扁平化后交给 finalize_run。 这样可以显著降低后端对单个 chord 的压力，提升成功率。
    # todo 什么意思？
    if len(sigs) <= max(1, CHORD_SPLIT_AT):
        return chord(group(sigs))(finalize_task_name.s(run_id)).id

    # 分层：把所有分片按 CHORD_SPLIT_AT 切成若干小组 → 小组内用 chord 收敛一次
    buckets = [sigs[i : i + CHORD_SPLIT_AT] for i in range(0, len(sigs), CHORD_SPLIT_AT)]

    # 小 chord：group(bucket) → chord → collect_bucket（原样返回该桶的结果列表）
    sub_chords = [
        chord(group(bucket), body=collect_bucket.s())
        for bucket in buckets
        if bucket
    ]

    # 外层 chord：等待所有小 chord 的回调完成后，先扁平化，再调用 finalize_run
    final_canvas = chord(group(sub_chords), body=(flatten_results.s() | finalize_task_name.s(run_id)))
    return final_canvas().id



# ====== “富行”解析：带条码/价格/原价/成本 ====== 当前没使用
"""
    返回字段：
      product_id, variant_id, sku, barcode,
      price, compare_at_price,
      cost_amount, cost_currency
"""
# def iter_variants_from_bulk_full(url: str) -> Iterator[Dict[str, Any]]:
    
#     with requests.get(url, stream=True, timeout=(10, 300), headers={"Accept-Encoding": "gzip, deflate"}) as r:
#         r.raise_for_status()
#         for line in r.iter_lines(decode_unicode=True):
#             if not line:
#                 continue
#             try:
#                 obj = json.loads(line)
#             except Exception:
#                 continue
#             if obj.get("__typename") != "ProductVariant":
#                 continue

#             inv = obj.get("inventoryItem") or {}
#             unit_cost = inv.get("unitCost") or {}
#             yield {
#                 "product_id": obj.get("__parentId"),
#                 "variant_id": obj.get("id"),
#                 "sku": (obj.get("sku") or "").strip(),
#                 "barcode": (obj.get("barcode") or "").strip(),
#                 "price": obj.get("price"),
#                 "compare_at_price": obj.get("compareAtPrice"),
#                 "cost_amount": unit_cost.get("amount"),
#                 "cost_currency": unit_cost.get("currencyCode"),
#             }



# ====== 仅要 (sku, variant_id) 的轻量解析 ======
"""
流式解析 Shopify Bulk 导出 JSONL 的生成器：
    - 用来从一条超大的 JSONL 文件里只提取关心的两个字段：sku 和 variant_id，
    - 并 一行一行边读边产出 (sku, variant_id) 二元组，供后续按 5k 一片去切分、投递分片任务
    - 内存友好：不把整份 JSONL 装入内存。
    - 用 requests.get(..., stream=True) 逐行读取 Shopify Bulk 的 JSONL
"""
# todo test
# 每行都会判定 __typename 或 id 是否包含 /ProductVariant/。只有确认是变体行才 yield (sku, variant_id)。
# Product 行、其它类型的节点会被跳过，所以我们处理的粒度就是“变体”
def iter_variant_from_bulk(url: str) -> Iterator[Tuple[str, str]]:
    
    # 建立 HTTP 流式请求
    with requests.get(url, stream=True, timeout=(10, 300), headers={"Accept-Encoding": "gzip, deflate"}) as r:
        r.raise_for_status()

        # 逐行读取 JSONL
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                row = json.loads(line)  # 将每行字符串解析成 JSON 对象
            except Exception:
                continue  # 行级 try/except，坏行直接跳过，不让整个任务失败

            # 过滤出“变体行”
            node_id = row.get("id")            # variant_id, 类似 gid://shopify/ProductVariant/1234567890
            typename = row.get("__typename")
            is_variant = (typename == "ProductVariant") or (isinstance(node_id, str) and "/ProductVariant/" in node_id)
            # 由于 iter_variant_from_bulk 跳过 Product 行，所以缓冲里不会混入“非变体”的条目。
            # 每个 (sku, variant_id) 代表一个准确的变体。一个商品有多个变体，就会产生多条记录，也符合分片逻辑
            if not is_variant:
                continue

            # 取出需要的两个字段
            sku = (row.get("sku") or "").strip()
            if sku and isinstance(node_id, str):
                yield (sku, node_id)



def iter_variant_from_bulk_head(url: str, limit: Optional[int] = None) -> Iterator[Tuple[str, str]]:
    """
    读取 JSONL 的前 limit 条记录；limit 为空则回退到完整结果。
    """
    if limit is None:
        yield from iter_variant_from_bulk(url)
        return
    remaining = max(0, int(limit))
    if remaining == 0:
        return
    gen = iter_variant_from_bulk(url)
    try:
        for idx, pair in enumerate(gen):
            if idx >= remaining:
                break
            yield pair
    finally:
        try:
            gen.close()
        except AttributeError:
            pass



''' 
运维接口使用
   - 只根据 manifest 重投 pending/failed 分片
   - 会创建一个新的 chord 并在完成后调用 finalize_run
'''
def schedule_chunks_from_manifest(
    run_id: str,
    *, statuses: tuple[str, ...] = ("pending", "failed"),
    process_task_name: str = "app.orchestration.product_sync.product_sync_task.process_chunk",
    finalize_task_name: str = "app.orchestration.product_sync.product_sync_task.finalize_run",
):
    db = SessionLocal()
    try:
        rows: List[ProductSyncChunk] = (
            db.query(ProductSyncChunk)
              .filter(ProductSyncChunk.run_id == run_id)
              .filter(ProductSyncChunk.status.in_(list(statuses)))
              .order_by(ProductSyncChunk.chunk_idx.asc())
              .all()
        )
    finally:
        db.close()

    inline_mode = _inline_tasks_enabled()

    if not rows:
        # 没有待重跑的分片，直接触发一次 finalize（让它走 manifest 汇总逻辑）
        if inline_mode:
            from app.orchestration.product_sync.product_sync_task import finalize_run
            return finalize_run.run([], run_id)
        return signature(finalize_task_name).delay([], run_id).id

    sigs = []
    inline_results: List[dict] = [] if inline_mode else None
    for r in rows:
        sku_codes = r.sku_codes or []
        task_id = f"ps:chunk:{run_id}:{r.chunk_idx}"
        if inline_mode:
            from app.orchestration.product_sync.product_sync_task import process_chunk
            inline_results.append(process_chunk.run(run_id, r.chunk_idx, sku_codes, False))
        else:
            sig = signature(process_task_name).s(run_id, r.chunk_idx, sku_codes, False).set(task_id=task_id)
            sigs.append(sig)

    if inline_mode:
        from app.orchestration.product_sync.product_sync_task import finalize_run
        return finalize_run.run(inline_results or [], run_id)

    if len(sigs) <= max(1, CHORD_SPLIT_AT):
        return chord(group(sigs))(finalize_task_name.s(run_id)).id

    buckets = [sigs[i : i + CHORD_SPLIT_AT] for i in range(0, len(sigs), CHORD_SPLIT_AT)]
    sub_chords = [chord(group(bucket), body=collect_bucket.s()) for bucket in buckets if bucket]
    final_canvas = chord(group(sub_chords), body=(flatten_results.s() | finalize_task_name.s(run_id)))
    return final_canvas().id




# ====== 小工具任务：汇总与扁平化 ======
@shared_task(name="app.tasks.product_sync.collect_bucket")
def collect_bucket(results):
    # 子 chord 的回调：直接原样返回该桶内所有分片的结果（List[Dict]）
    return results


@shared_task(name="app.tasks.product_sync.flatten_results")
def flatten_results(nested):
    # 将 List[List[Dict]] 扁平为 List[Dict]
    flat = []
    for part in nested or []:
        if isinstance(part, list):
            flat.extend(part)
        elif part is not None:
            flat.append(part)
    return flat
