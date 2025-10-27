

from __future__ import annotations
from celery import shared_task, group, chord
from celery.exceptions import SoftTimeLimitExceeded
import time, math, json, requests, os, redis, pytz, hashlib, logging
from datetime import datetime, date
from typing import List, Tuple
from app.utils.attrs_hash import calc_attrs_hash_current

from app.core.config import settings
from app.db.session import SessionLocal
from app.db.model.product import ProductSyncRun

from app.integrations.shopify.shopify_client import run_bulk_products_by_tag, current_bulk_operation
from app.integrations.dsz import get_products_by_skus_with_stats, normalize_dsz_product
from app.repository.product_repo import ( 
    load_existing_by_skus, diff_snapshot, bulk_upsert_sku_info, save_candidates,)
from app.orchestration.freight_calculation.freight_task import kick_freight_calc




# ============== 可调参数（有默认值，没在 settings 配就用默认） ==============
USE_CHORD = True          # True=用 Celery chord；False=用 Redis 计数器
POLL_MAX_RETRIES = 45
POLL_DELAY_SEC   = 30
SYNC_CHUNK_SKUS: int = getattr(settings, "sync_chunk_skus", 5000)
DSZ_BATCH_SIZE: int = getattr(settings, "dsz_batch_size", 50)
DSZ_GET_TIMEOUT  = getattr(settings, "dsz_get_timeout", 180)   # orchestrator 阻塞等待子任务超时（秒）

# Redis 计数器 key（仅 USE_CHORD=False 时用）
REDIS_URL = getattr(settings, "REDIS_URL", None) or getattr(settings, "CELERY_BROKER_URL", "redis://redis:6379/0")
REDIS_KEY_CHUNKS_LEFT = "run:{run_id}:chunks_left"
REDIS_KEY_CHANGED_SUM = "run:{run_id}:changed_sum"


logger = logging.getLogger(__name__)



# ============== 入口：提交 Shopify Bulk ==============
"""
1) 创建 run 记录 product_sync_runs
2) 向 Shopify 提交 bulk query / BulkOperation RunQuery: 只查tag商品, 获取id和sku信息
3) Webhook 为主接收shopify通知, 安排轮询兜底 Polling 为辅
"""
@shared_task(name = "app.orchestration.product_sync.sync_start_full")
def sync_start_full() -> str:

    TAG = getattr(settings, "SHOPIFY_TAG_FULL_SYNC", "DropShippingZone")
    db = SessionLocal()

    try:
        # 1. 插入一条 run 记录（状态 running）
        run = ProductSyncRun(status="running", run_type="full_sync")
        db.add(run); db.commit(); db.refresh(run)

        # 2. 发起 Shopify Bulk
        resp = run_bulk_products_by_tag(TAG)
        bulk_id = (resp.get("data") or {}).get("bulkOperationRunQuery", {}).get("bulkOperation", {}).get("id")
        # 3. 存下返回的 bulk_id
        run.shopify_bulk_id = bulk_id   
        db.commit()

        # 4. 等 Webhook 回调带 URL

        # 5. 兜底轮询（30s 后开始，每 POLL_DELAY_SEC 秒一次，最多 POLL_MAX_RETRIES 次）
        # 把“轮询一次”的任务丢到 Celery 队列里，不阻塞当前调用方
        poll_bulk_until_ready.apply_async(args=[run.id], countdown=POLL_DELAY_SEC)
        
        return run.id   #run.id（用于后续查询该次同步的状态）
    finally:
        db.close()



# ========== Webhook 驱动的后半程 - 轮询回调：拿 URL → 切片 → 编排 ========
"""
Webhook / 兜底轮询都会调用到这里：
    1) 记录 bulk 状态 & 下载 JSONL
    2) 切成 5k/片
    3) 用 chord / 或 Redis 计数器 安排所有分片执行
使用celery任务, 为了让Webhook 接口立刻接收返回
"""
@shared_task(
    name="app.orchestration.product_sync.handle_bulk_finish",
    bind=True,
    max_retries=6,                  # 最多 6 次
    default_retry_delay=15,         # 首次 15s；配合 backoff/jitter 形成 15s,30s,60s,120s...
    retry_backoff=True,             # 指数退避
    retry_jitter=True,              # 抖动，避免雪崩
    autoretry_for=(requests.exceptions.RequestException,),
    soft_time_limit=300,            # 软时限 5 分钟
    time_limit=360,                 # 硬时限 6 分钟（兜底）
)
def handle_bulk_finish(self, bulk_id: str, url: str, object_count: int | None = None):

    db = SessionLocal()

    try:
        #  1. 根据bulkID 找到对应的 run 记录
        run = db.query(ProductSyncRun).filter_by(shopify_bulk_id=bulk_id).first()
        if not run:
            run = ProductSyncRun(shopify_bulk_id=bulk_id, status="running")
            db.add(run); db.commit(); db.refresh(run)
        
        # 2. 若重复调用（幂等）：已有 URL 就直接返回
        if run.shopify_bulk_url:
            return {"run_id": run.id, "skipped": "url already saved"}

        # 3. 记录状态更新
        run.shopify_bulk_status = "COMPLETED"
        
        # 4. 更新 Bulk 状态/URL/数量
        run.shopify_bulk_url = url
        if object_count is not None:
            run.total_shopify_skus = int(object_count or 0)
        db.commit()

        # 直接下载 JSONL，提取 sku, variant_id [(sku, variant_id), ...]
        # pairs = _download_sku_variant_pairs(url)
        # pairs = _schedule_chunks_streaming();
        # run.total_shopify_skus = len(pairs); 
        # db.commit()
        # return _schedule_chunks(run.id, pairs)

        # 第二种方式
        return _schedule_chunks_streaming(run.id, url)
    
    except SoftTimeLimitExceeded as e:
        raise self.retry(exc = 3, countdown = 20)
    finally:
        db.close()



# ========= 切片 & 编排 把 (sku,vid) 列表按 5k/片切分并安排执行 =========
"""
 - USE_CHORD=True  → chord(group(process_chunk), finalize_run)
 - USE_CHORD=False → Redis 计数器：
   chunks_left=片数；每片完成时 DECR; 到 0 触发 finalize_run_noresults
"""
def _schedule_chunks(run_id: str, pairs: list[tuple[str, str]]):

    # 1. 分片
    parts = [pairs[i:i + SYNC_CHUNK_SKUS] for i in range(0, len(pairs), SYNC_CHUNK_SKUS)]
    if not parts:
        finalize_run.delay([], run_id)  
        return {"run_id": run_id, "chunks": 0}  # 空数据也要收尾
    
    # 2. 编排 计数
    if USE_CHORD:
        tg = group(process_chunk.s(run_id, part, use_counter=False) for part in parts)
        # finalize_run 会收到一个 list[process_chunk 的返回值] + 你额外透传的 run_id
        return chord(tg)(finalize_run.s(run_id)).id
    
    # Redis 计数器方案
    # r = _get_redis()
    # r.set(REDIS_KEY_CHUNKS_LEFT.format(run_id=run_id), len(parts))
    # r.set(REDIS_KEY_CHANGED_SUM.format(run_id=run_id), 0)

    # 非 chord 模式：用计数器（这里用 “收尾汇总/回传结果” 即可，无需 Redis）
    # 直接并行执行；finalize_run 会汇总每片返回
    for part in parts:
        process_chunk.delay(run_id, part, True)
    return {"run_id": run_id, "chunks": len(parts), "mode": "no-chord"}



# ============== 每个分片内部流程 ==============
'''
处理一个“入口分片”（比如 5000 个 SKU):
   step 1 - 去 DSZ 批拉详情、标准化
   step 2 - 与数据库快照做 Diff
   step 3 - 执行 Upsert, 并写入 候选变更
'''
@shared_task(
        name="app.tasks.product_full_sync.process_chunk", 
        soft_time_limit=600,   # 保留超时保护；不做分片级自动重试
)
def process_chunk(run_id: str, sku_variant_pairs: list[tuple[str, str]], use_counter: bool = False):
    db = SessionLocal()

    try:
        skus = [s for s,_ in sku_variant_pairs]
        vid_map = dict(sku_variant_pairs)

        # 1. 读现有快照（仅 SYNC_FIELDS）
        old_map = load_existing_by_skus(db, skus)

        changed_rows: list[dict] = []     # 仅变更的行用于 upsert
        candidate_tuples: list[tuple[str, dict]] = []  # (sku, new_partial_fields)


        # 2. DSZ 分批拉取
        for i in range(0, len(skus), DSZ_BATCH_SIZE):
            codes = skus[i: i + DSZ_BATCH_SIZE]

            # 受限速 & 专用队列：阻塞等待返回，确保“单 worker + rate_limit”成立
            # 每个分批内, 根据skuCode 查询 DSZ API
            rows = _dsz_fetch_blocking(codes)  

            # 标准化并补充 variant 映射
            normed = []
            for raw in rows:
                n = normalize_dsz_product(raw)
                n["shopify_variant_id"] = vid_map.get(n["sku_code"])
                # 生成 attrs_hash_current（基于 5.3 入参白名单 & 促销有效性）
                n["attrs_hash_current"] = calc_attrs_hash_current(n)
                normed.append(n)

            # 对每条做 diff: 只把“有变化”的行推进 changed_rows & candidates
            for n in normed:
                sku = n["sku_code"]
                old = old_map.get(sku)
                d = diff_snapshot(old, n)  # 返回 {field: {old,new}}，key 集合即变更字段

                if d:
                    # 将整行加入 upsert 列表（避免漏写 attrs_hash_current 等字段）
                    changed_rows.append(n)

                    # 候选：用“变更字段 + attrs_hash_current”构建最小新值集
                    mask_keys = set(d.keys()) | {"attrs_hash_current"}

                    # 仅提取“变更字段的新值子集”，交由 save_candidates 落库
                    new_partial = {k: n.get(k) for k in d.keys()}
                    candidate_tuples.append((sku, new_partial))  


        # 3. Upsert：仅对 changed_rows 写库（减少写放大）
        if changed_rows:
            bulk_upsert_sku_info(db, changed_rows, only_update_when_changed=True)
            db.commit()

        # 4. 变更的sku作为candidate, 保存DB，供 5.3 增量优先
        if candidate_tuples:
            save_candidates(db, run_id, candidate_tuples)
            db.commit()

        return {"changed": len(changed_rows), "candidates": [sku for sku, _new in candidate_tuples]}
    finally:
        db.close()



# ================== 汇总结果并收尾 （Chord/无Chord通用） ==================
# 把这次运行标记为 completed，记录 finished_at
# 并触发下游（ 运费计算任务 kick_freight_calc.delay(run_id)）
"""
results: 每个分片返回的 dict(见 process_chunk 的 return)。
汇总 changed、聚合候选 SKU, 标记 run 完成，并触发 5.3(运费计算)且优先处理候选。
 """
# 看不懂？
@shared_task(name="app.tasks.product_full_sync.finalize_run")
def finalize_run(results, run_id: str):
    total_changed = 0

    sku_set: set[str] = set()
    for r in results or []:
        total_changed += int((r or {}).get("changed", 0))
        for s in ((r or {}).get("candidates") or []):
            sku_set.add(s)

    db = SessionLocal()
    try:
        run = db.get(ProductSyncRun, run_id)
        if run:
            run.status = "completed"
            run.finished_at = datetime.utcnow()
            run.changed_count = total_changed
            db.commit()
    finally:
        db.close()

    try:
        # 把候选 SKU 直接传给 5.3，做“增量优先”
        kick_freight_calc.delay(str(run_id), candidate_skus=list(sku_set), trigger="product-sync-trigger")

    except Exception:
        pass

    return {"run_id": run_id, "changed": total_changed, "candidate_skus": len(sku_set)}




# ============== shopify工具方法 ==============
# todo 没懂？
"""
下载 JSONL 并解析出 (sku, variant_id) 列表
    从 Shopify Bulk 的 JSONL 结果里提取 (sku, product_variant_id) 列表。
    - Bulk 结果是“逐行 JSON（NDJSON）”，父子节点被“扁平化”：
        * Product 行: 没有 __parentId
        * ProductVariant 行: 有 __typename == "ProductVariant"，并可能带 __parentId 指向其 Product
    - 我们只关心变体行(Variant)：从行内直接取 sku 和 id（gid://shopify/ProductVariant/...）。
    - 这样避免构建父子映射，内存占用最小；40k+ 商品也能轻松处理。

    返回值:
        List[Tuple[sku:str, variant_id:str]]
"""
def _download_sku_variant_pairs(url: str) -> List[Tuple[str, str]]:

    pairs: List[Tuple[str, str]] = []

    # 提前允许 gzip/deflate，长连接流式读取；(connect, read) 超时分开更稳
    # 不会把文件先整包下载到本地。它用 requests.get(..., stream=True) 一边下载一边逐行解析（NDJSON）
    # HTTP 连接+读取分开超时；stream=True 不会一次性把响应内容读完
    with requests.get(
        url,
        stream=True,
        timeout=(10, 300),
        headers={"Accept-Encoding": "gzip, deflate"},
    ) as r:
        r.raise_for_status()

        # decode_unicode=True 直接得到 str；keepends=False（默认）去掉行尾
        for line in r.iter_lines(decode_unicode=True):
            if not line:  # 空行
                continue
            try:
                row = json.loads(line)
            except Exception:
                # 单行损坏/截断：跳过，不影响整体
                continue

            # 判断是否为“变体行”
            typename = row.get("__typename")
            node_id = row.get("id")

            is_variant = (
                typename == "ProductVariant"
                or (isinstance(node_id, str) and "/ProductVariant/" in node_id)
            )

            if not is_variant:
                # Product 行或其它类型，直接跳过
                continue

            sku = (row.get("sku") or "").strip()
            vid = row.get("id")

            # 只收有 sku 且有 id 的变体
            if sku and isinstance(vid, str):
                # 每解析到一行就 append 进去, 会吃内存的是把全部 pair 存入 list
                pairs.append((sku, vid))      

    return pairs




# ================== Shopify查询结果 兜底轮询 ==================
""" 
如果 webhook 丢失，轮询 currentBulkOperation 拿 url, 进入后半程
兜底轮询 Shopify Bulk 状态，直到完成/失败；完成后下载 JSONL 得到产品和变体信息。
最多重试 60 次，每次 30 秒，大约 ~30 分钟窗口。时间够不够？
"""
@shared_task(
    name="app.tasks.product_full_sync.poll_bulk_until_ready",
    bind=True, max_retries=POLL_MAX_RETRIES, default_retry_delay=POLL_DELAY_SEC
)
def poll_bulk_until_ready(self, run_id: str):
    db = SessionLocal()

    try:
        run = db.get(ProductSyncRun, run_id)
        if not run:
            return "no run"

        # 若已拿到 url（可能 webhook 已处理），直接跳过
        if run.shopify_bulk_url:
            return "already has url"
        
        # 拉取 status/url/objectCount
        info = current_bulk_operation()
        op = (info.get("data") or {}).get("currentBulkOperation") or {}
        status = op.get("status")
        url = op.get("url")
        object_count = op.get("objectCount")

        run.shopify_bulk_status = status
        db.commit()

        # 1 - 若 FAILED/CANCELED，标记 run 失败退出
        # 兜底轮询读到 Shopify Bulk 状态为 FAILED/CANCELED 时，代码会把本次 run 标记为失败并返回
        # todo：之后做什么？
        if status in ("FAILED", "CANCELED"):
            run.status = "failed"; db.commit()
            return f"bulk {status.lower()}"
        
        # 2 - 若 COMPLETED 且有 url：
        if status == "COMPLETED" and url:
            # 直接走与 webhook 相同的处理逻辑
            handle_bulk_finish.delay(run.shopify_bulk_id or op.get("id"), url, object_count)
            return "handled via polling"

        # 未完成 → 重试（可选：指数退避）
        delay = min(60, self.default_retry_delay * (1.2 ** self.request.retries))
        raise self.retry(countdown=int(delay))
    finally:
        db.close()




# =================== （可选）Sweeper 兜底扫表 ===================
"""
给没拿到 URL 的 run 再触发一次轮询
每 2 分钟跑一次（在 beat 里配置），找出 “running && 有 bulk_id 但没 url”的 run, 
再触发一次 poll 兜底。
 """
@shared_task(name="app.tasks.product_full_sync.bulk_url_sweeper")
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
            poll_bulk_until_ready.delay(rid)

        return {"scheduled": len(ids)}
    finally:
        db.close()




# ======= 阻塞式调用 DSZ 受限队列（确保限流与单 worker 生效）=======
"""
    通过 Celery result backend 阻塞等待 dsz_fetch 返回。
    - 强制投递到 dsz_io 队列，受 rate_limit & 并发=1 约束。
    - 若输入超过 DSZ_BATCH_SIZE，自动拆分为多个 dsz_fetch 并行提交（dsz_io 实际串行消费），
      避免你在上层再手动切分这一层。
    """
def _dsz_fetch_blocking(codes: List[str]) -> list[dict]:
    if not codes:
        return []
    # 不超过批大小：直接一次阻塞拿结果
    if len(codes) <= DSZ_BATCH_SIZE:
        async_result = dsz_fetch.apply_async(args=[codes], queue="dsz_io")
        return async_result.get(timeout=DSZ_GET_TIMEOUT)
    
    # 超过批大小：自动拆分再合并
    sigs = []
    for i in range(0, len(codes), DSZ_BATCH_SIZE):
        chunk = codes[i : i + DSZ_BATCH_SIZE]
        sigs.append(dsz_fetch.s(chunk).set(queue="dsz_io"))

    grp = group(sigs)()
    parts = grp.get(timeout=DSZ_GET_TIMEOUT)  # List[List[dict]]
    merged: list[dict] = []
    for p in (parts or []):
        if p:
            merged.extend(p)
    return merged



# =========== DSZ 请求任务（单队列 + 限流） ===========
'''
真正打 DSZ 的单元是一个单独 Celery 任务 dsz_fetch
把 dsz_fetch 放在单独的队列，并且让只有一个 worker( 并发=1) 来消费这个队列，就等价于
“所有 DSZ 请求串行过这一条管道”，总速率就会被 rate_limit="60/m" 卡住，不会变成 多个 × 60
单批 DSZ 拉取任务：
    - 入参 codes 数量超过 DSZ_BATCH_SIZE 时，这里会截断（上层已拆分，不应触发）。
    - 捕获 RequestException 自动重试; SoftTimeLimitExceeded 也做一次 retry。
    - 只做“纯 IO”, 不做业务组装, 便于排查与限流。
'''
@shared_task(
    name="app.tasks.product_full_sync.dsz_fetch",
    rate_limit="60/m",  # 单 worker 生效；建议此队列并发=1, # 配合 dsz_io 队列的单并发 worker，确保总速率约束
    max_retries=5,
    default_retry_delay=10,                        # 首次重试间隔，可配合 retry_backoff 使用
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,                            # 指数退避：10s, 20s, 40s, ...
    retry_jitter=True,                             # 加抖动，避免雪崩
    soft_time_limit=60,                            # 子任务自身软超时
)
def dsz_fetch(self, codes: list[str]) -> list[dict]:
    from app.integrations.dsz import fetch_dsz_by_codes
    try:
        if not codes:
            return []
        if len(codes) > DSZ_BATCH_SIZE:
            codes = codes[:DSZ_BATCH_SIZE]

        return fetch_dsz_by_codes(codes)

    except SoftTimeLimitExceeded as e:
        # 如果 DSZ 响应过慢，给一次短暂重试机会
        raise self.retry(exc=e, countdown=15)
    



#  流式切片调度：攒够一个切片就发一个 process_chunk；最后用 chord 汇总
def _schedule_chunks_streaming(run_id: str, url: str, chunk_size: int = SYNC_CHUNK_SKUS):
    buf = []
    sigs = []
    for pair in iter_variant_pairs_from_bulk(url):
        buf.append(pair)
        if len(buf) >= chunk_size:
            sigs.append(process_chunk.s(run_id, buf, False))
            buf = []
    if buf:
        sigs.append(process_chunk.s(run_id, buf, False))

    if not sigs:
        finalize_run.delay([], run_id)
        return {"run_id": run_id, "chunks": 0}

    # 仍然使用 chord 来“等全部分片完成再收口”
    return chord(group(sigs))(finalize_run.s(run_id)).id



#  生成器：逐行产出 (sku, variant_id)，不落整表到内存
def iter_variant_pairs_from_bulk(url: str):
    import requests, json
    with requests.get(url, stream=True, timeout=(10, 300), headers={"Accept-Encoding": "gzip, deflate"}) as r:
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            node_id = row.get("id")
            typename = row.get("__typename")
            is_variant = (typename == "ProductVariant") or (isinstance(node_id, str) and "/ProductVariant/" in node_id)
            if not is_variant:
                continue
            sku = (row.get("sku") or "").strip()
            if sku and isinstance(node_id, str):
                yield (sku, node_id)