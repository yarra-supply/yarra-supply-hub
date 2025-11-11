
# feature: 每 1~5 分钟由 beat 触发一次，read schedule 表：
# 命中触发窗口则投递 products_full_sync.sync_start_full

from __future__ import annotations
import datetime as dt
import pytz
from celery import shared_task
from sqlalchemy import text
from app.db.session import SessionLocal

from app.core.config import settings
from app.orchestration.product_sync.product_sync_task import sync_start_full
from app.orchestration.price_reset.price_reset import kick_price_reset


DOW_TO_INT = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
WINDOW_MINUTES = 10  

"""
feature: 每 N 分钟跑一次（见 Celery beat 配置）
    - 读取 schedule_items: key='product_full_sync', enabled, day_of_week(0~6), hour, minute, every_2_weeks(bool), timezone(IANA), last_run_at(UTC)
    - 在目标时间±10分钟内触发一次 sync_start_full()，并更新 last_run_at
    - 所有开关/时间/隔周逻辑集中在 scheduler_tick.tick()
"""
@shared_task(name="app.orchestration.scheduler_tick.tick_product_full_sync")
def tick_product_full_sync():

    # 1) 读取默认时区（字符串）并构造 tz 对象
    tzname_str = getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne")
    tz_default = pytz.timezone(tzname_str)
    now_local = dt.datetime.now(tz_default)

    db = SessionLocal()

    try:
        # 2) 读取一行配置, select key='product_full_sync' first record
        row = db.execute(text(
            """
            SELECT key, enabled, day_of_week, hour, minute, every_2_weeks, timezone, last_run_at
            FROM schedules
            WHERE key = 'product_full_sync'
            LIMIT 1
            """
        )).mappings().first()

        if not row:
            return {"status": "no-schedule-item"}
        if not row["enabled"]:
            return {"status": "disabled"}
        
        # 3) 按DB记录自带时区重算“现在”: 再拿一次“当前本地时间”。这么做可让每个任务按各自时区跑
        tz_local = pytz.timezone(row.get("timezone") or tzname_str)
        now_local = dt.datetime.now(tz_local)

        # 4) day_of_week 支持 'MON'..'SUN'；如是 int 也兼容
        dow_val = row["day_of_week"]
        if isinstance(dow_val, str):
            dow_int = DOW_TO_INT[dow_val.upper()]
        else:
            dow_int = int(dow_val)

        # 5) 计算本周目标触发时刻 target: 命中触发窗口（窗口 10 分钟）
        target = _target_dt_this_week(now_local, dow_int, row["hour"], row["minute"])

        # 6）判断是否落在 10 分钟触发窗口: beat 每 N 分钟跑一次，但只在一个窄窗口内触发一次，避免重复触发
        in_window = target <= now_local < (target + dt.timedelta(minutes=WINDOW_MINUTES))
        if not in_window:
            return {"status": "outside-window"}


        # 6) 隔周闸门 todo 可以改成每周?
        if row.get("every_2_weeks") and not _pass_biweekly_gate(now_local, row.get("last_run_at")):
                return {"status": "biweekly-gate-skip"}
        
        # 7) 二次幂等保护：同一窗口内只触发一次
        if row.get("last_run_at"):
            last_local = row["last_run_at"].astimezone(tz_local)
            if (now_local - last_local) < dt.timedelta(minutes=WINDOW_MINUTES):
                return {"status": "already-fired-in-window"}

        # 8) 触发入口: 投递异步任务，不阻塞当前 tick
        sync_start_full.delay()

        # 9) 更新 last_run_at（UTC）
        db.execute(
            text("UPDATE schedules SET last_run_at = :now WHERE key = 'product_full_sync'"),
            {"now": dt.datetime.now(dt.timezone.utc)},
        )
        db.commit()

        return {"status": "fired"}
    finally:
        db.close()



'''
每周价格重置定时任务
    - 每 N 分钟执行一次
    - 读取 schedule_items(key='price_reset')，在目标时间±10分钟命中后触发 kick_price_reset()
    - 支持 every_2_weeks 与 timezone 字段，与 full_sync 的 tick 保持一致
'''
@shared_task(name="app.orchestration.scheduler_tick.tick_price_reset")
def tick_price_reset():

    # 1) get local time and timezone
    tzname_str = getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne")
    tz_default = pytz.timezone(tzname_str)
    now_local = dt.datetime.now(tz_default)

    db = SessionLocal()

    try:
        # 2) 读取一行配置, select key='price_reset' first record
        row = db.execute(text(
            """
            SELECT key, enabled, day_of_week, hour, minute, every_2_weeks, timezone, last_run_at
            FROM schedules
            WHERE key = 'price_reset'
            LIMIT 1
            """
        )).mappings().first()

        if not row:
            return {"status": "no-schedule-item"}
        if not row["enabled"]:
            return {"status": "disabled"}
        
        # 3) 按DB记录自带时区重算“现在”: 再拿一次“当前本地时间”。这么做可让每个任务按各自时区跑
        tz_local = pytz.timezone(row.get("timezone") or tzname_str)
        now_local = dt.datetime.now(tz_local)

        # 4) day_of_week 支持 'MON'..'SUN'；如是 int 也兼容
        dow_val = row["day_of_week"]
        if isinstance(dow_val, str):
            dow_int = DOW_TO_INT[dow_val.upper()]
        else:
            dow_int = int(dow_val)

        # 5) 计算本周目标触发时刻 target: 命中触发窗口（窗口 10 分钟）
        target = _target_dt_this_week(now_local, dow_int, row["hour"], row["minute"])

        # 5）判断是否落在 10 分钟触发窗口: beat 每 N 分钟跑一次，但只在一个窄窗口内触发一次，避免重复触发
        in_window = target <= now_local < (target + dt.timedelta(minutes=WINDOW_MINUTES))
        if not in_window:
            return {"status": "outside-window"}
        
        # 6) 隔周闸门 todo 可以改成每周?
        if row.get("every_2_weeks") and not _pass_biweekly_gate(now_local, row.get("last_run_at")):
            return {"status": "biweekly-gate-skip"}
        
        # 7）二次幂等保护：同一窗口内只触发一次
        if row.get("last_run_at"):
            last_local = row["last_run_at"].astimezone(tz_local)
            if (now_local - last_local) < dt.timedelta(minutes=WINDOW_MINUTES):
                return {"status": "already-fired-in-window"}

        # 8) 触发价格还原入口: 投递异步任务，不阻塞当前 tick
        kick_price_reset.delay()

        # 9) 更新 last_run_at（UTC 带时区）
        db.execute(
            text("UPDATE schedules SET last_run_at = :now WHERE key = 'price_reset'"),
            {"now": dt.datetime.now(dt.timezone.utc)},
        )
        db.commit()
        return {"status": "fired"}
    finally:
        db.close()



# 计算本周目标触发时刻
def _target_dt_this_week(now_local: dt.datetime, dow: int, hour: int, minute: int) -> dt.datetime:
    # 以本地星期一 00:00 为起点，推到目标周几/时间
    base = now_local - dt.timedelta(days=now_local.weekday(),
                                    hours=now_local.hour,
                                    minutes=now_local.minute,
                                    seconds=now_local.second,
                                    microseconds=now_local.microsecond)
    return base + dt.timedelta(days=dow, hours=hour, minutes=minute)


'''
隔周闸门
   - 轻量隔周实现：不靠日期差，而靠 ISO 周的奇偶。跨年也考虑了（年变更即放行）
'''
def _pass_biweekly_gate(now_local: dt.datetime, last_run_at_utc: dt.datetime|None) -> bool:
    
    # 如果从未跑过（last_run_at 为空），直接可以运行
    if not last_run_at_utc:
        return True
    
    # 把 last_run_at（UTC）转为当前时区，再分别取 isocalendar() 的周序号, 
    last_local = last_run_at_utc.astimezone(now_local.tzinfo)
    y1, w1, _ = last_local.isocalendar()
    y2, w2, _ = now_local.isocalendar()

    # 只有当奇偶性变化（本周序与上次不同奇偶）时放行
    return (w2 % 2) != (w1 % 2) or (y1 != y2)
