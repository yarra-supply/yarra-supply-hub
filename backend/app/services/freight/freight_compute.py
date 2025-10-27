# 运费计算服务

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Iterable, Mapping
from statistics import median
import hashlib, math
from decimal import Decimal, ROUND_HALF_UP


# --------- 常量与工具 ----------
STATES_ALL = ("ACT","NSW_M","NSW_R","QLD_M","QLD_R","SA_M","SA_R","TAS_M","TAS_R","VIC_M","VIC_R","WA_M")
STATES_METRO = ("ACT","NSW_M","QLD_M","SA_M","TAS_M","VIC_M","WA_M")
STATES_RURAL = ("NSW_R","QLD_R","SA_R","TAS_R","VIC_R","WA_R")
NZ_KEY = "freight_nz"  # 供未来扩展；当前计算聚焦 AU
SENTINEL_NO_SERVICE = 9000  # 9999/9000 视为无服务，可放配置或 DB



def _d(val) -> Optional[Decimal]:
    if val is None: return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

def _round(val: Optional[Decimal], places: str) -> Optional[Decimal]:
    if val is None: return None
    return val.quantize(Decimal(places), rounding=ROUND_HALF_UP)

def _avg(values: list[Decimal]) -> Optional[Decimal]:
    vals = [v for v in values if v is not None]
    if not vals: return None
    return sum(vals) / Decimal(len(vals))


def _cfgD(cfg: Optional[Mapping[str, any]], key: str, default: float | int | str) -> Decimal:
    """读取 cfg[key] 并转 Decimal；为空则用 default。"""
    if cfg is None:
        return Decimal(str(default))
    val = cfg.get(key) if isinstance(cfg, Mapping) else None
    return Decimal(str(val)) if val is not None else Decimal(str(default))


def _cfgI(cfg: Optional[Mapping[str, any]], key: str, default: int) -> int:
    """读取 cfg[key] 并转 int；为空则用 default。"""
    if cfg is None:
        return default
    val = cfg.get(key) if isinstance(cfg, Mapping) else None
    return int(val) if val is not None else default



# --------- 输入 / 输出模型 ----------
@dataclass
class FreightInputs:
    # 价格（DSZ）
    price: Optional[float]                 # regular price = skuInfo 的 dsz_price
    special_price: Optional[float] = None  # 可能已被预处理过期置空

    # 各州运费 & 远程
    state_freight: Dict[str, Optional[float]] = None  # 需包含: 上面的 12 州 + "REMOTE" + 可选 "NZ"

    # 重量与体积（变量字段.txt 使用 CBM；若无，可传 None）
    weight: Optional[float] = None
    cbm: Optional[float] = None


@dataclass
class FreightOutputs:
    # 统计变量（表内“变量字段”）
    adjust: Optional[Decimal]
    same_shipping: Optional[Decimal]
    shipping_ave: Optional[Decimal]
    m_shipping_ave: Optional[Decimal]
    r_shipping_ave: Optional[Decimal]

    shipping_med: Optional[Decimal]
    remote_check: bool
    rural_ave: Optional[Decimal]
    weighted_ave_s: Optional[Decimal]
    shipping_med_dif: Optional[Decimal]
    cubic_weight: Optional[Decimal]
    shipping_type: str

    # 按“calculate weight”规则计算
    weight: Optional[Decimal]

    # 定价
    selling_price: Optional[Decimal]
    shopify_price: Optional[Decimal]
    kogan_au_price: Optional[Decimal]
    kogan_k1_price: Optional[Decimal]
    kogan_nz_price: Optional[Decimal]



# --------- 逐项计算（完全对齐《变量字段.txt》/PDF 公式） ----------
"""
Adjust: 若 Selling Price < 25, 取其 4%；否则为空。:contentReference[oaicite:5]{index=5}
"""
def compute_adjust(selling_price: Optional[Decimal]) -> Optional[Decimal]:
    if selling_price is None: return None
    return _round(selling_price * Decimal("0.04"), "0.01") if selling_price < Decimal("25") else None

# Adjust 计算：用配置驱动
# def compute_adjust(
#     selling_price: Optional[Decimal],
#     cfg: Optional[Mapping[str, object]] = None,
# ) -> Optional[Decimal]:
#     if selling_price is None:
#         return None
#     threshold = _cfgD(cfg, "adjust_threshold", 25.0)
#     rate      = _cfgD(cfg, "adjust_rate", 0.04)
#     if selling_price < threshold:
#         return (selling_price * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
#     return None

# what？？
def _values_for(keys: tuple[str, ...], fr: Dict[str, Optional[float]]) -> list[Optional[Decimal]]:
    return [_d(fr.get(k)) for k in keys]


def compute_same_shipping(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    SameShipping: 各州(不含 WA_R)最大值 - 最小值。:contentReference[oaicite:6]{index=6}
    用 12 个州（不含 WA_R)运费的最大值减最小值
    """
    vals = [v for v in _values_for(STATES_ALL, fr) if v is not None]
    if len(vals) < 2: return None
    return max(vals) - min(vals)


def compute_shipping_ave(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    ShippingAve: 上述各州（不含 WA_R)平均,保留 1 位小数。:contentReference[oaicite:7]{index=7}
    """
    return _round(_avg(_values_for(STATES_ALL, fr)), "0.0")


def compute_m_shipping_ave(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    MShippingAve:Metro 平均 (ACT, NSW_M, QLD_M, SA_M, TAS_M, VIC_M, WA_M), 1 位小数。
    """
    return _round(_avg(_values_for(STATES_METRO, fr)), "0.0")


def compute_r_shipping_ave(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    RShippingAve:Rural 平均 (NSW_R, QLD_R, SA_R, TAS_R, VIC_R, WA_R),1 位小数。
    """
    return _round(_avg(_values_for(STATES_RURAL, fr)), "0.0")


def compute_shipping_med(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    ShippingMed: 全国各州（不含 WA_R) 运费中位数
    """
    vals = [v for v in _values_for(STATES_ALL, fr) if v is not None]
    if not vals: return None
    return _d(median(vals))


def compute_remote_check(fr: Dict[str, Optional[float]]) -> bool:
    """
    RemoteCheck: REMOTE ∈ {999, 9999} 或 WA_R=9999 → True。
    REMOTE 为 999 或 9999、或 WA_R 为 9999 时视为偏远不送
    """
    remote = _d(fr.get("REMOTE"))
    wa_r = _d(fr.get("WA_R"))
    return (remote in {Decimal("999"), Decimal("9999")}) or (wa_r == Decimal("9999"))


def compute_rural_ave(remote_check: bool, fr: Dict[str, Optional[float]], shipping_ave: Optional[Decimal]) -> Optional[Decimal]:
    """
    RuralAve: 若 RemoteCheck==1, 用 ShippingAve;否则取平均([REMOTE], [WA_R]), 1 位小数。
    """
    if remote_check == 1:
        return shipping_ave
    return _round(_avg([_d(fr.get("REMOTE")), _d(fr.get("WA_R"))]), "0.0")


def compute_weighted_ave_s(remote_check: bool, shipping_ave: Optional[Decimal], rural_ave: Optional[Decimal]) -> Optional[Decimal]:
    """
    WeightedAveS：RemoteCheck==1 → ShippingAve；否则 ShippingAve*0.95 + RuralAve*0.05（1 位小数）。
    """
    if shipping_ave is None: return None
    if remote_check == 1: 
        return shipping_ave
    if rural_ave is None: 
        return None
    return _round(shipping_ave * Decimal("0.95") + rural_ave * Decimal("0.05"), "0.0")


def compute_shipping_med_dif(
        fr: Dict[str, Optional[float]], shipping_med: Optional[Decimal]) -> Optional[Decimal]:
    """
    ShippingMedDif：max(REMOTE - ShippingMed, WA_M - ShippingMed)
    """
    if shipping_med is None: return None
    remote = _d(fr.get("REMOTE"))
    wa_m = _d(fr.get("WA_M"))
    diffs = [v - shipping_med for v in (remote, wa_m) if v is not None]

    if not diffs: return None
    return max(diffs)


def compute_cubic_weight(weight: Optional[float], cbm: Optional[float]) -> Optional[Decimal]:
    """
    CubicWeight：若 weight 或 CBM 为空 → null；
    否则若 weight > (CBM*250 - 1) → null；
    否则 CubicWeight = round(CBM*250, 2)。:contentReference[oaicite:15]{index=15}
    """
    w = _d(weight); c = _d(cbm)
    if w is None or c is None: return None

    if w > (c * Decimal("250") - Decimal("1")):
        return None
    return _round(c * Decimal("250"), "0.01")


"""
    ShippingType：与 M 代码一致的分段判断
    关键变量：
      - priceRatio = sameShipping / price
      - ruralDiff = ruralAve - shippingMed
      - remoteMedDiff = REMOTE - shippingMed
    规则见《变量字段.txt》“Added Custom10”段。
    输出: "0" | "1" | "10" | "15" | "20" | "Extra2|3|4|5"
 """
def compute_shipping_type(
    shipping_ave: Optional[Decimal], # 没用上 
    same_shipping: Optional[Decimal],
    shipping_med: Optional[Decimal],
    rural_ave: Optional[Decimal],
    shipping_med_dif: Optional[Decimal],
    remote_check: bool,
    price: Optional[float],
    fr: Dict[str, Optional[float]],
) -> str:
    if any(x is None for x in (same_shipping, rural_ave, shipping_med, shipping_med_dif)):
        # 兜底：信息不足也要给出分型
        return "Extra3"

    price_dec = _d(price)
    price_ratio = (same_shipping / price_dec) if (price_dec and price_dec != 0) else None
    rural_diff = rural_ave - shipping_med
    remote_med_diff = None
    if _d(fr.get("REMOTE")) is not None:
        remote_med_diff = _d(fr.get("REMOTE")) - shipping_med

    cond1 = (price_ratio is not None) and (price_ratio < Decimal("0.3")) and (same_shipping < Decimal("15")) and (shipping_med_dif < Decimal("15"))
    cond2 = (shipping_med_dif < Decimal("20"))

    # 原始分支翻译
    if rural_ave == Decimal("0"):
        result = "0"
    elif same_shipping == Decimal("0") and (rural_diff < Decimal("30.1") or remote_check):
        result = "1"
    elif same_shipping < Decimal("10.1") and (rural_diff < Decimal("30.1") or remote_check):
        result = "10"
    elif same_shipping < Decimal("30.1") and (rural_diff < Decimal("30.1") or remote_check):
        if shipping_med == Decimal("0"):
            result = "15" if (cond1 and ((remote_med_diff is not None and remote_med_diff < Decimal("15")) or remote_check)) else "Extra2"
        elif shipping_med > Decimal("0"):
            result = "20" if (cond2 and ((remote_med_diff is not None and remote_med_diff < Decimal("20")) or remote_check)) else "Extra2"
        else:
            result = "Extra2"
    else:
        if same_shipping < Decimal("50"): result = "Extra3"
        elif same_shipping < Decimal("100"): result = "Extra4"
        else: result = "Extra5"

    return str(result)


# --------- 新增：calculate weight ----------
"""
    对应《计算公式.txt》末尾“calculate weight”规则：
    - 仅当 ShippingType 属于 Extra3/Extra4/Extra5 时计算，否则为 null
    - MaxWeight = max(Weight(kg), CubicWeight)
    - 若 MaxWeight == 0 或 ShippingMed == 0 → 取 ShippingMed/1.5
    - 否则 CalcWeight = ShippingMed/1.5；若 |CalcWeight - MaxWeight| / MaxWeight <= 0.15 → 用 MaxWeight，否则用 CalcWeight
    - 若最终结果为 0 则置空
    - 结果保留两位小数
"""
def compute_weight(
    shipping_type: str,
    weight: Optional[float],
    cubic_weight: Optional[Decimal],
    shipping_med: Optional[Decimal],
) -> Optional[Decimal]:
    
    st = (shipping_type or "").strip()
    is_extra = any(tag in st for tag in ("Extra3", "Extra4", "Extra5"))
    if not is_extra:
        return None

    w = _d(weight) or Decimal("0")
    cw = cubic_weight or Decimal("0")
    sm = shipping_med or Decimal("0")

    max_weight = max(w, cw)

    # 若 MaxWeight 或 ShippingMed 为 0
    if max_weight == 0 or sm == 0:
        result = (sm / Decimal("1.5")) if sm != 0 else None
        return None if (result is None or result == 0) else _round(result, "0.01")

    calc_weight = sm / Decimal("1.5")
    # 避免除 0，上面已保证 max_weight > 0
    ratio_diff = (calc_weight - max_weight).copy_abs() / max_weight

    result = max_weight if ratio_diff <= Decimal("0.15") else calc_weight
    if result == 0:
        return None
    return _round(result, "0.01")




# --------- 价格计算 ----------
"""
生效价格：有 Special Price 用 Special, 否则用 regular price
如果special_date明天到期，则不使用special_price? 
"""
# 使用第一个公式的地方？
def compute_selling_price(price: Optional[float], special_price: Optional[float]) -> Optional[Decimal]:
    sp = _d(special_price)
    rg = _d(price)
    return sp if sp is not None else rg


def compute_shopify_price(selling_price: Optional[Decimal]) -> Optional[Decimal]:
    """
    Shopify Price：Selling Price < 25 用 1.26，否则 1.22；保留两位小数。
    这个就是用DSZ配置的shopify规则计算的
    """
    if selling_price is None: return None
    mult = Decimal("1.26") if selling_price < Decimal("25") else Decimal("1.22")
    return _round(selling_price * mult, "0.01")


"""
    Kogan AUPrice：按 ShippingType 套用逆算/加价公式
"""
def compute_kogan_au_price(
    selling_price: Optional[Decimal],
    shipping_type: str,
    vic_m: Optional[float],
    shipping_med: Optional[Decimal],
    weighted_ave_s: Optional[Decimal],
) -> Optional[Decimal]:
    
    if selling_price is None: return None
    vic = _d(vic_m) or Decimal("0")
    med_m = shipping_med or Decimal("0")
    w_as = weighted_ave_s or Decimal("0")

    st = str(shipping_type)
    if st == "Extra2":
        base = (selling_price + w_as) / Decimal("0.82")
    elif st in ("Extra3", "Extra4"):
        base = (selling_price / Decimal("0.82")) if vic == 0 else (selling_price + vic / Decimal("2")) / Decimal("0.82")
    elif st == "Extra5":
        base = ((selling_price / Decimal("0.82")) if vic == 0 else (selling_price + vic / Decimal("2")) / Decimal("0.82")) / Decimal("0.969")
    else:
        # 普通：<25 用 0.79，否则 0.82
        denom = Decimal("0.79") if selling_price < Decimal("25") else Decimal("0.82")
        base = (selling_price + med_m) / denom

    return _round(base, "0.01")


def compute_k1_price(kogan_au_price: Optional[Decimal]) -> Optional[Decimal]:
    """
    K1 Price：若 Kogan AUPrice > 66.7 → *0.969；否则减 2.01
    """
    if kogan_au_price is None: return None
    return _round(kogan_au_price * Decimal("0.969"), "0.01") if kogan_au_price > Decimal("66.7") else _round(kogan_au_price - Decimal("2.01"), "0.01")


def compute_kogan_nz_price(selling_price: Optional[Decimal], nz_cost: Optional[float]) -> Optional[Decimal]:
    """
    Kogan NZPrice：NZ==9999 → null；否则 round((Selling + NZ)/(1-0.08-0.12)/0.9, 2)
    """
    if selling_price is None: return None
    nz = _d(nz_cost)
    if nz is None or nz == Decimal("9999"):  # 9999 表示不送
        return None
    return _round((selling_price + nz) / (Decimal("1") - Decimal("0.08") - Decimal("0.12")) / Decimal("0.9"), "0.01")




# --------- 顶层：一次性计算一行 SKU 的全部变量 ----------
def compute_all(i: FreightInputs, 
                cfg: Optional[Mapping[str, any]] = None) -> FreightOutputs:

    fr = i.state_freight or {}

    # todo 替换字段到DB cfg 功能测试完再换就行
    selling_price = compute_selling_price(i.price, i.special_price)                                   # 生效价格
    adjust = compute_adjust(selling_price)                                                            # 低价调整

    same_shipping = compute_same_shipping(fr)
    shipping_ave = compute_shipping_ave(fr)
    m_shipping_ave = compute_m_shipping_ave(fr)
    r_shipping_ave = compute_r_shipping_ave(fr)
    shipping_med = compute_shipping_med(fr)

    remote_check = compute_remote_check(fr)
    rural_ave = compute_rural_ave(remote_check, fr, shipping_ave)
    weighted_ave_s = compute_weighted_ave_s(remote_check, shipping_ave, rural_ave)
    shipping_med_dif = compute_shipping_med_dif(fr, shipping_med)
    cubic_weight = compute_cubic_weight(i.weight, i.cbm)

    shipping_type = compute_shipping_type(
        shipping_ave, same_shipping, shipping_med, rural_ave, shipping_med_dif,
        remote_check, i.price, fr
    )

    # 新增：weight（calculate weight）
    weight = compute_weight(
        shipping_type=shipping_type,
        weight=i.weight,
        cubic_weight=cubic_weight,
        shipping_med=shipping_med,
    )

    shopify_price = compute_shopify_price(selling_price)
    kogan_au_price = compute_kogan_au_price(selling_price, shipping_type, fr.get("VIC_M"), shipping_med, weighted_ave_s)
    kogan_k1_price = compute_k1_price(kogan_au_price)
    kogan_nz_price = compute_kogan_nz_price(selling_price, fr.get("NZ"))

    return FreightOutputs(
        adjust=adjust,
        same_shipping=same_shipping,
        shipping_ave=shipping_ave,
        m_shipping_ave=m_shipping_ave,
        r_shipping_ave=r_shipping_ave,
        shipping_med=shipping_med,
        remote_check=remote_check,
        rural_ave=rural_ave,
        weighted_ave_s=weighted_ave_s,
        shipping_med_dif=shipping_med_dif,
        cubic_weight=cubic_weight,
        shipping_type=shipping_type,
        weight=weight,       # ← 新增字段返回
        selling_price=selling_price,
        shopify_price=shopify_price,
        kogan_au_price=kogan_au_price,
        kogan_k1_price=kogan_k1_price,
        kogan_nz_price=kogan_nz_price,
    )