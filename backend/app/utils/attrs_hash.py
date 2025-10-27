
from __future__ import annotations
import hashlib
from datetime import datetime, date
from decimal import Decimal
from copy import deepcopy
import pytz
from app.core.config import settings



# ---------- 影响运费计算的字段（用于 candidate 过滤） ----------
# 把 FREIGHT_RELEVANT_FIELDS 作为“入参字段白名单”，对其按固定顺序序列化后做哈希，得到 attrs_hash_current
# 唯一权威白名单：凡是会影响 【运费/定价结果的入参字段】，都在这里
FREIGHT_HASH_FIELDS = (
    
    "price", "special_price", "special_price_end_date",  # 价格（含促销有效性）
    
    "length", "width", "height", "weight",          # 尺寸/重量, CBM不使用，都用 L*W*H/1,000,000 计算
    
    "freight_act",                                  # 运费（含 NZ、REMOTE；NT 做兼容保留）
    "freight_nsw_m", "freight_nsw_r",
    "freight_nt_m", "freight_nt_r",
    "freight_qld_m", "freight_qld_r",
    "remote",
    "freight_sa_m", "freight_sa_r",
    "freight_tas_m", "freight_tas_r",
    "freight_vic_m", "freight_vic_r",
    "freight_wa_m", "freight_wa_r",
    "freight_nz",
)

_AU_TZ = pytz.timezone(getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne"))



'''
Sku hash: attrs_hash_current, 组成字段：FREIGHT_HASH_FIELDS
freight hash: attrs_hash_last_calc, 就是上一次的sku hash
'''

"""
计算“当前属性哈希”，用于快速判断是否对运费/价格敏感的属性发生变化。
注意：此函数不会修改传入的 snapshot（内部会做浅拷贝）。
"""
def calc_attrs_hash_current(snapshot: dict) -> str:
    
    snap = deepcopy(snapshot)
    
    # 1) 修改special_price的有效性
    _apply_special_price_validity(snap)

    # 2) 拼接为稳定字符串后做 sha256
    parts = [f"{k}={_normalize_for_hash(snap.get(k))}" for k in FREIGHT_HASH_FIELDS]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()



"""
    若 special_price_end_date 已过期（严格早于“今天”的日期；到期日当天仍有效），
    则把 special_price 设置为 price（而非 None）。
"""
def _apply_special_price_validity(snapshot: dict) -> None:

    end = snapshot.get("special_price_end_date")
    if not end:
        return
    
    try:
        # 解析到“日期”层面进行比较（到期日当天仍有效）
        if isinstance(end, datetime):
            end_date = (end if end.tzinfo else _AU_TZ.localize(end)).astimezone(_AU_TZ).date()
        else:
            # 字符串或其他类型，截取前 10 位尝试按 YYYY-MM-DD 解析
            end_date = datetime.strptime(str(end)[:10], "%Y-%m-%d").date()

        today = datetime.now(_AU_TZ).date()

        # 仅当到期日严格早于今天时视为过期；若想 00:00 当天失效，可改为 <=
        if end_date < today:
            # 过期后特殊价回落为常规价；若 price 不存在则相当于清空特价
            snapshot["special_price"] = snapshot.get("price")
    except Exception:
        pass  ## 容错：解析失败不阻断流程



'''
辅助函数：将不同类型的值规范化为稳定字符串：
    - datetime/date → 'YYYY-MM-DD'（按 Australia/Melbourne）
    - Decimal → 保留两位
    - int/float → 6 位有效数字，避免浮点表示噪声
    - 其他 → 去首尾空白的 str
'''
def _normalize_for_hash(v) -> str:
    if v is None:
        return ""
    
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = _AU_TZ.localize(v)
        return v.astimezone(_AU_TZ).strftime("%Y-%m-%d")
    
    if isinstance(v, Decimal):
        return str(v.quantize(Decimal("0.01")))
    
    if isinstance(v, (int, float)):
        return f"{v:.6g}"
    return str(v).strip()