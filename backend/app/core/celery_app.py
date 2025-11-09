
# 分钟 Tick + 动态调度

from celery import Celery
from kombu import Exchange, Queue
from app.core.config import settings
from app.core.logging import configure_logging

configure_logging()


'''
初始化 Celery 应用/实例
   - Beat/Orchestrator: 1 台
   - Worker: 2-3 台 (8~12 并发/台)
'''
celery_app = Celery(
    "yarra_supply_hub",
    broker=settings.CELERY_BROKER_URL,          # 队列位置 (Redis)
    backend=settings.CELERY_RESULT_BACKEND,     # 结果存储 (Redis)
    include=[
        # 让 Celery 在启动时就加载这些模块, include 告诉 Celery 这些模块里定义的任务函数要自动注册
        "app.orchestrator.scheduler_tick",                          # 定时调度任务
        "app.orchestrator.product_sync.product_sync_task",          # 商品全量同步
        "app.orchestrator.retry_sweeper",                           # 扫失败重试

        "app.orchestrator.price_reset.price_reset",                 # 周三价格回滚
        "app.orchestrator.freight_calculation.freight_task",        # 运费计算
        # "app.orchestrator.dispatch_shopify.dispatch_shopify_task",      # Shopify同步任务
    ],
)


'''
  通用 Celery 配置
'''
celery_app.conf.update(
    timezone=settings.CELERY_TIMEZONE,           # 时区，默认澳洲时间
    enable_utc=True,                             # 内部还是存 UTC
    task_serializer="json",                      # 序列化格式 JSON
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,                     # 任务启动时标记 started
    broker_connection_retry_on_startup=True,     # 启动时如果 broker 挂了会重试
    # === 容错和超时控制 ===
    worker_prefetch_multiplier=1,    # 一个 worker 一次只取一个任务
    task_acks_late=True,             # worker crash 后任务会回队列/任务会重新分配 防止任务丢失,任务执行完再确认，异常可重投
    # task_time_limit=60 * 20,         # 最长运行 20 分钟（硬超时）不适合：需要释放外部资源/写回状态/删临时文件的任务（比如有 DSZ/Shopify 的长 I/O、分页拉取、文件生成等）。
    # task_soft_time_limit=60 * 18,    # 18 分钟发软中断，留 2 分钟清理, 按每个任务真实耗时来定。
    # result_expires=3600,             # 结果保存 1 小时
    broker_heartbeat=30,             # 和 broker 的心跳，防掉线
    broker_pool_limit=10,            # 连接池大小（按并发规模调）
)



'''
不同任务配置不同队列
   - 务类型差异很大, DSZ API 调用：慢 I/O, 容易堵塞。
   - DB upsert: CPU/内存为主。Freight 计算：大批量运算。所以 拆分队列
'''
celery_app.conf.task_queues = (
    Queue("default", Exchange("default"), routing_key="default"),

    Queue("orchestrator", Exchange("orchestrator"), routing_key="orchestrator"),   # 商品同步编排相关任务
    Queue("dsz_io", Exchange("dsz_io"), routing_key="dsz_io"),                     # 专门跑 DSZ API 的任务

    Queue("freight", Exchange("freight"), routing_key="freight"),                  # 专门跑 运费计算

    # Queue("dispatch", Exchange("dispatch"), routing_key="dispatch"),             # 专门跑 Shopify 字段更新
)


'''
celery 路由规则 - 就像“转发规则”：
   - dsz_fetch → dsz_io 队列
   - freight_task → freight 队列
   - dispatch_shopify_task → dispatch 队列
这样 Celery 可以把不同的任务分配到不同队列
'''
celery_app.conf.task_routes = {
    # 定时调度任务
    "app.orchestrator.scheduler_tick.tick_product_full_sync": {"queue": "orchestrator"},
    "app.orchestrator.scheduler_tick.tick_price_reset": {"queue": "orchestrator"},

    # 周三价格回滚
    "app.orchestrator.price_reset.kick_price_reset": {"queue": "orchestrator"},

    # 商品同步编排相关任务
    "app.orchestrator.product_sync.sync_start_full": {"queue": "orchestrator"},
    "app.orchestrator.product_sync.handle_bulk_finish": {"queue": "orchestrator"},
    "app.orchestrator.product_sync.poll_bulk_until_ready": {"queue": "orchestrator"},
    #todo 其他2个要写吗？
    "app.orchestrator.product_sync.bulk_url_sweeper": {"queue": "orchestrator"},

    # DSZ API 调用（限流队列；请用单独 worker 并发=1 消费它）
    # todo 现在不用了？
    "app.orchestrator.products_full_sync.dsz_fetch": {"queue": "dsz_io"},

    # 运费计算
    "app.orchestrator.freight_calculation.kick_freight_calc": {"queue": "freight"},
    "app.orchestrator.freight_calculation.freight_calc_run": {"queue": "freight"},

    # Shopify 出站
    # "app.orchestrator.dispatch_shopify.dispatch_shopify_task.*": {"queue": "dispatch"},
}



# 默认的静态调度
celery_app.conf.beat_schedule = {
    
    # 每 5 分钟由 DB 决定是否触发全量同步（具备开关/时间/隔周闸门）
    "db-schedule-tick": {
        "task": "app.orchestrator.scheduler_tick.tick",
        "schedule": 300,  # 秒
    },

    "db-schedule-price-reset": {
        "task": "app.orchestrator.scheduler_tick.tick_price_reset",
        "schedule": 300,
        # "options": {"queue": "orchestrator"}
    },

    # B) 兜底：每 2 分钟扫一遍未拿到 URL 的 run，触发轮询（强烈推荐保留）
    # "bulk-url-sweeper": {
    #     "task": "app.orchestrator.product_sync.product_sync_task.bulk_url_sweeper",
    #     "schedule": crontab(minute="*/2"),
    # },

    # C) 失败扫表（默认 5 分钟一次）
    # "retry-sweeper": {
    #     "task": "app.orchestrator.retry_sweeper.sweep_failures", 
    #     "schedule": {"__type__": "crontab", "minute": "*/5"},
    # },
}

# === Beat 配置 ===
# 线上用 RedBeat（基于 Redis 的分布式 beat）来替代 Celery 自带的 beat
# celery_app.conf.beat_scheduler = "redbeat.RedBeatScheduler"


# ---- 自动发现 ----
# 若任务分散在多个子包里，autodiscover 能保证 worker 找到它们
celery_app.autodiscover_tasks(packages=[
    "app.tasks",
])
