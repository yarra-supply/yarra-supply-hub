
from __future__ import annotations
import os
from typing import List, Dict, Optional
import time
import logging
from celery import shared_task
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.core.config import settings
from app.core.logging import configure_logging

from app.services.freight.freight_cal_config_loader import load_freight_calc_config
from app.services.freight.freight_cal_service import process_batch_compute_and_persist

from app.repository.freight_repo import (
    create_freight_run,
    finish_freight_run,
    get_candidate_skus_from_run,
    filter_need_recalc,
)
from app.db.model.product import SkuInfo
from app.db.model.freight import FreightRun


configure_logging()
logger = logging.getLogger(__name__)


# 单批处理窗口（根据机器/DB 压力调整）
BATCH_SIZE = settings.FREIGHT_BATCH_SIZE



'''
 触发运费计算流程
 - 来自 商品同步流程 finalize_run（自动）或运维按钮（手动）
 - candidate_skus 若为空：默认只计算“哈希有变化”的 SKU（DB 自检）
'''
@shared_task(name="app.orchestration.freight_calculation.freight_task.kick_freight_calc")
def kick_freight_calc(product_run_id: Optional[str] = None, trigger: str = "manual"):

    logger.info("========  kick_freight_calc start product_run_id=%s  ========", product_run_id)
    
    db = SessionLocal()

    try:
        # 创建运费计算记录 （仓储函数内目前会 commit，一次很短的事务）统一按“拉模式”：候选数量先记 0，真正数量在后续任务中回填
        run_id = create_freight_run(db, product_run_id, trigger, 0)
    finally:
        db.close()

    # todo 测试使用
    # freight_calc_run.run(run_id, product_run_id, trigger)

    # 把 run_id、product_run_id 传下去
    freight_calc_run.run(run_id, product_run_id, trigger)
    
    logger.info("======== kick_freight_calc end ========")
    return {"freight_run_id": run_id}



"""
真正的计算任务：
    1) 若未提供候选，则自动筛选 attrs_hash_current 变更的 SKU
    2) 分批 compute + upsert
    3) 提交事务后写 Shopify 作业，再由 5.5 Dispatcher 消费

典型场景是 4万+ SKU 的批处理；一个超大事务会长时间持锁，影响并发, 小步提交能：
降低锁持有时间（每批只锁这批数据，提交就释放），内存与 redo 量更小，出错只丢当前批，之前已提交的批次保留下来
"""
@shared_task(name="app.orchestration.freight_calculation.freight_task.freight_calc_run", 
             bind=True, max_retries=3, default_retry_delay=15)
def freight_calc_run(
    self, 
    freight_run_id: str, 
    product_run_id: Optional[str],
    trigger: str
):

    db: Session = SessionLocal()  
    changed_total = 0

    try:
        # 1) 确定目标sku集合
        if product_run_id:         # 若传入product_run_id, 从 product_sync_candidates 读取候选 SKU
            target_skus = get_candidate_skus_from_run(db, product_run_id)

            # if "HR-AIR-AUTO-20M" in target_skus:
            #     # 打印观察信息
            #     print(f"Found HR-AIR-AUTO-20M in target_skus, total={len(target_skus)}")
            #     try:
            #         sku_info = db.query(SkuInfo).filter(SkuInfo.sku_code == "HR-AIR-AUTO-20M").first()
            #         if sku_info:
            #             # 过滤掉内部属性，打印可读字段
            #             print({k: v for k, v in sku_info.__dict__.items() if not k.startswith("_")})
            #         else:
            #             print("SkuInfo not found in DB for HR-AIR-AUTO-20M")
            #     except Exception as e:
            #         print("Error fetching SkuInfo for HR-AIR-AUTO-20M:", e)

            # todo：为了幂等/去重，再次做哈希过滤更稳妥, 之后放开？
            #target_skus = filter_need_recalc(db, target_skus)
        else: 
            # 无 run_id：兜底/手动触发，按“哈希差异”做增量筛选（只算“确实变了”的 SKU）
            # test 分支         
            subq = db.query(SkuInfo.sku_code).all()
            all_skus = [r.sku_code for r in subq]
            # sku_info.attrs_hash_current ≠ kogan_sku_freight_fee.attrs_hash_last_calc 做增量筛选，只算确实变了的SKU
            target_skus = filter_need_recalc(db, all_skus)


        # 2) 把统计到的候选数回填到 FreightRun（短事务提交）
        run = db.get(FreightRun, freight_run_id)
        if run:
            run.candidate_count = len(target_skus)
            db.commit()

        if not target_skus:  # 若候选为空：立即把本次 run 标记为 completed 并返回
            finish_freight_run(db, freight_run_id, status="completed", changed_count=0, message="no sku to calc")
            return {"freight_run_id": freight_run_id, "changed": 0}
        

        # 3) 查询 freight_calc_config 运费计算参数
        cfg = load_freight_calc_config(db)


        # 4) 分批处理：进入批次循环时，Session 会自动开始一个新事务
        loop_start = time.perf_counter()

        for i in range(0, len(target_skus), BATCH_SIZE):
            iteration_start = time.perf_counter()
            batch = target_skus[i:i + BATCH_SIZE]


            # 如果本批次包含特定 SKU，则打印观察信息
            # if "HR-AIR-AUTO-20M" in batch:
            #     logger.info(
            #         "Found HR-AIR-AUTO-20M in batch_index=%d batch_global_range=%d-%d batch_size=%d",
            #         (i // BATCH_SIZE) + 1,
            #         i,
            #         i + len(batch) - 1,
            #         len(batch),
            #     )
            #     try:
            #         sku_info = db.query(SkuInfo).filter(SkuInfo.sku_code == "HR-AIR-AUTO-20M").first()
            #         if sku_info:
            #             logger.info("SkuInfo: %s", {k: v for k, v in sku_info.__dict__.items() if not k.startswith("_")})
            #         else:
            #             logger.info("SkuInfo not found in DB for HR-AIR-AUTO-20M")
            #     except Exception as e:
            #         logger.exception("Error fetching SkuInfo for HR-AIR-AUTO-20M: %s", e)

            # 运费计算 + DB更新
            changed = process_batch_compute_and_persist(db, batch, freight_run_id, cfg=cfg, trigger=trigger)
            if changed:
                db.commit()      # ← 第N次事务提交（小步提交）

                # 小批次提交后，# 如需唤醒派发器，可在此发一个轻量任务（示例）
                # notify_shopify.delay(freight_run_id, batch_count=len(batch))
            changed_total += changed
            iteration_elapsed = time.perf_counter() - iteration_start

            logger.info("freight_calc_run batch=%d size=%d changed=%d elapsed=%.2fs",
                (i // BATCH_SIZE) + 1, len(batch), changed, iteration_elapsed,)
            
        total_elapsed = time.perf_counter() - loop_start
        total_batches = (len(target_skus) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(
            "freight_calc_run all batches finished batches=%d total_skus=%d changed_total=%d elapsed=%.2fs",
            total_batches,
            len(target_skus),
            changed_total,
            total_elapsed,
        )

        # 3) 完成, update status of freight run, 一个很短的事务
        finish_freight_run(db, freight_run_id, status="completed", changed_count=changed_total)
        # notify_shopify(freight_run_id, batch_count=0)   # todo 通知shopify同步任务
        return {"freight_run_id": freight_run_id, "changed": changed_total}

    except Exception as e:
        db.rollback()   # ← 回滚“当前未提交”的那一批
        finish_freight_run(db, freight_run_id, status="failed", changed_count=changed_total, message=str(e))
        raise
    finally:
        db.close()  # ← 归还连接



'''
  运费计算完成后, 触发通知shopify进行第三阶段的任务
  通知 Shopify 同步任务去消费 ShopifyUpdateJob 表中的作业（5.5）。
    - 默认只传 run_id，派发器自己按 available_at/status 拉取可用作业；
    - batch_count 仅用于日志/可观测（可选）。
'''
# todo 加task 不加 .delay报错
# def notify_shopify(freight_run_id: str, batch_count: int = 0) -> None:
#     if _dispatch_task is None:
#         return
#     try:
#         _dispatch_task.delay(trigger="freight", related_run_id=freight_run_id, batch_count=batch_count)
#     except Exception:
#         # 通知失败不阻塞 5.3；派发器通常有定时兜底
#         pass
