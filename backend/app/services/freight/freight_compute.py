# 运费计算服务

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Iterable, Mapping
from statistics import median
import hashlib, logging, math
from decimal import ROUND_HALF_EVEN, Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from app.core.config import settings


logger = logging.getLogger(__name__)


# --------- 常量与工具 ----------
STATES_ALL = ("ACT","NSW_M","NSW_R","QLD_M","QLD_R","SA_M","SA_R","TAS_M","TAS_R","VIC_M","VIC_R","WA_M")
STATES_METRO = ("ACT","NSW_M","QLD_M","SA_M","TAS_M","VIC_M","WA_M")
STATES_RURAL = ("NSW_R","QLD_R","SA_R","TAS_R","VIC_R","WA_R")
NZ_KEY = "freight_nz"  # 供未来扩展；当前计算聚焦 AU
SENTINEL_NO_SERVICE = 9000  # 9999/9000 视为无服务，可放配置或 DB
_FREIGHT_TZ = ZoneInfo(getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne"))



def _d(val) -> Optional[Decimal]:
    if val is None: return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

# todo 所有字段都受影响，check 所有字段
def _round(val: Optional[Decimal], places: str) -> Optional[Decimal]:
    """Power Query 公式默认使用 Banker's rounding（HALF_EVEN），保持一致。"""
    if val is None: return None
    return val.quantize(Decimal(places), rounding=ROUND_HALF_EVEN)

def _avg(values: list[Decimal]) -> Optional[Decimal]:
    vals = [v for v in values if v is not None]
    if not vals: return None
    return sum(vals) / Decimal(len(vals))


_Q_CENTS = Decimal("0.01")
_Q_THOUSAND = Decimal("0.001")
_Q_RATIO = Decimal("0.0001")
# _MAX_NUMERIC_14_3 = Decimal("99999999999.999")


# 量化函数：按不同精度量化 为了和DB保持一致
def _quantize(val: Optional[Decimal], quantum: Decimal) -> Optional[Decimal]:
    if val is None:
        return None
    return val.quantize(quantum, rounding=ROUND_HALF_UP)


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


def _values_for(keys: tuple[str, ...], fr: Dict[str, Optional[float]]) -> list[Optional[Decimal]]:
    return [_d(fr.get(k)) for k in keys]



# --------- 输入 / 输出模型 ----------
@dataclass
class FreightInputs:
    # 价格相关
    price: Optional[float] = None
    special_price: Optional[float] = None
    special_price_end_date: Optional[Any] = None

    # 尺寸/重量
    # length: Optional[float] = None
    # width: Optional[float] = None
    # height: Optional[float] = None
    weight: Optional[float] = None
    cbm: Optional[float] = None
    # 幂等字段
    attrs_hash_current: Optional[str] = None

    # 各州运费（17 个字段 + remote + nz）
    act: Optional[float] = None
    nsw_m: Optional[float] = None
    nsw_r: Optional[float] = None
    nt_m: Optional[float] = None
    nt_r: Optional[float] = None
    qld_m: Optional[float] = None
    qld_r: Optional[float] = None
    remote: Optional[float] = None
    sa_m: Optional[float] = None
    sa_r: Optional[float] = None
    tas_m: Optional[float] = None
    tas_r: Optional[float] = None
    vic_m: Optional[float] = None
    vic_r: Optional[float] = None
    wa_m: Optional[float] = None
    wa_r: Optional[float] = None
    nz: Optional[float] = None

    @property
    def state_freight(self) -> Dict[str, Optional[float]]:
        """将 ORM 载入的分州运费字段映射为统一字典。"""
        return {
            "ACT": self.act,
            "NSW_M": self.nsw_m,
            "NSW_R": self.nsw_r,
            "NT_M": self.nt_m,
            "NT_R": self.nt_r,
            "QLD_M": self.qld_m,
            "QLD_R": self.qld_r,
            "SA_M": self.sa_m,
            "SA_R": self.sa_r,
            "TAS_M": self.tas_m,
            "TAS_R": self.tas_r,
            "VIC_M": self.vic_m,
            "VIC_R": self.vic_r,
            "WA_M": self.wa_m,
            "WA_R": self.wa_r,
            "REMOTE": self.remote,
            "NZ": self.nz,
        }


@dataclass
class FreightOutputs:
    # 统计变量（表内“变量字段”）
    adjust: Optional[Decimal]
    same_shipping: Optional[Decimal]
    shipping_ave: Optional[Decimal]
    shipping_ave_m: Optional[Decimal]
    shipping_ave_r: Optional[Decimal]

    shipping_med: Optional[Decimal]
    remote_check: bool
    rural_ave: Optional[Decimal]
    weighted_ave_s: Optional[Decimal]
    shipping_med_dif: Optional[Decimal]
    cubic_weight: Optional[Decimal]
    shipping_type: str

    # 按“calculate weight”规则计算
    weight: Optional[Decimal]
    price_ratio: Optional[Decimal]

    # 定价
    selling_price: Optional[Decimal]
    shopify_price: Optional[Decimal]
    kogan_au_price: Optional[Decimal]
    kogan_k1_price: Optional[Decimal]
    kogan_nz_price: Optional[Decimal]



# --------- 逐项计算（完全对齐《变量字段.txt》/PDF 公式） ----------
"""
Adjust: 若 Selling Price < 25, 取其 4%；否则为空
"""
def compute_adjust(
    selling_price: Optional[Decimal],
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    if selling_price is None: return None
    threshold = _cfgD(cfg, "adjust_threshold", 25.0)
    rate = _cfgD(cfg, "adjust_rate", 0.04)
    return _round(selling_price * rate, "0.01") if selling_price < threshold else None


def compute_same_shipping(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    SameShipping: 各州(不含 WA_R)最大值 - 最小值
    用 12 个州（不含 WA_R)运费的最大值减最小值
    """
    vals = [v for v in _values_for(STATES_ALL, fr) if v is not None]
    if len(vals) < 2: return None
    return max(vals) - min(vals)


def compute_shipping_ave(fr: Dict[str, Optional[float]]) -> Optional[Decimal]:
    """
    ShippingAve: 上述各州（不含 WA_R)平均,保留 1 位小数
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


def compute_remote_check(
    fr: Dict[str, Optional[float]],
    cfg: Optional[Mapping[str, any]] = None,
) -> bool:
    """
    RemoteCheck: REMOTE ∈ {999, 9999} 或 WA_R=9999 → True。
    REMOTE 为 999 或 9999、或 WA_R 为 9999 时视为偏远不送
    """
    remote1 = _cfgD(cfg, "remote_1", 999)
    remote2 = _cfgD(cfg, "remote_2", 9999)
    wa_r_sentinel = _cfgD(cfg, "wa_r", 9999)
    remote = _d(fr.get("REMOTE"))
    wa_r = _d(fr.get("WA_R"))
    return (remote in {remote1, remote2}) or (wa_r == wa_r_sentinel)


def compute_rural_ave(remote_check: bool, fr: Dict[str, Optional[float]], shipping_ave: Optional[Decimal]) -> Optional[Decimal]:
    """
    RuralAve: 若 RemoteCheck==1, 用 ShippingAve;否则取平均([REMOTE], [WA_R]), 1 位小数。
    """
    if remote_check == 1:
        return shipping_ave
    return _round(_avg([_d(fr.get("REMOTE")), _d(fr.get("WA_R"))]), "0.0")


def compute_weighted_ave_s(
    remote_check: bool,
    shipping_ave: Optional[Decimal],
    rural_ave: Optional[Decimal],
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    """
    WeightedAveS：RemoteCheck==1 → ShippingAve；否则 ShippingAve*0.95 + RuralAve*0.05（1 位小数）。
    """
    if shipping_ave is None: return None
    if remote_check == 1: 
        return shipping_ave
    if rural_ave is None: 
        return None
    weight_shipping = _cfgD(cfg, "weighted_ave_shipping_weights", 0.95)
    weight_rural = _cfgD(cfg, "weighted_ave_rural_weights", 0.05)
    return _round(shipping_ave * weight_shipping + rural_ave * weight_rural, "0.0")


def compute_shipping_med_dif(
        fr: Dict[str, Optional[float]], shipping_med: Optional[Decimal]) -> Optional[Decimal]:
    """
    ShippingMedDif：max(REMOTE - ShippingMed, WA_R - ShippingMed)
    """
    if shipping_med is None: return None
    remote = _d(fr.get("REMOTE"))
    wa_r = _d(fr.get("WA_R"))
    diffs = [v - shipping_med for v in (remote, wa_r) if v is not None]

    if not diffs: return None
    return max(diffs)




"""
    ShippingType：复刻最新“Added Custom10”逻辑：
      - meetsRuralCondition := ShippingMedDif < 40 或 RemoteCheck 为真
      - meetsPriceRatio := PriceRatio < 0.45（可配置）
      - conditionGroup1 := ShippingMedDif < 10
      - conditionGroup2 := ShippingMedDif < 20
    输出: "0" | "1" | "10" | "20" | "Extra2|3|4|5"
 """
def compute_shipping_type(
    same_shipping: Optional[Decimal],
    rural_ave: Optional[Decimal],
    shipping_med_dif: Optional[Decimal],
    remote_check: bool,
    price: Optional[float],
    # selling_price: Optional[float],
    cfg: Optional[Mapping[str, any]] = None,
) -> tuple[str, Optional[Decimal]]:
    
    price_dec = _d(price)
    # price_dec = _d(selling_price)
    price_ratio_limit = _cfgD(cfg, "price_ratio", 0.45)
    price_ratio = None
    if price_dec and price_dec != 0 and rural_ave is not None:
        price_ratio = rural_ave / price_dec

    same_0 = _cfgD(cfg, "same_shipping_0", 0.0)
    same_10 = _cfgD(cfg, "same_shipping_10", 10.1)
    same_20 = _cfgD(cfg, "same_shipping_20", 20.1)
    same_30 = _cfgD(cfg, "same_shipping_30", 30.1)
    same_50 = _cfgD(cfg, "same_shipping_50", 50.0)
    same_100 = _cfgD(cfg, "same_shipping_100", 100.0)
    med_dif_10 = _cfgD(cfg, "med_dif_10", 10.0)
    med_dif_20 = _cfgD(cfg, "med_dif_20", 20.0)
    med_dif_40 = _cfgD(cfg, "med_dif_40", 40.0)

    med_dif = shipping_med_dif
    meets_rural_condition = (med_dif is not None and med_dif < med_dif_40) or bool(remote_check)
    meets_price_ratio = (price_ratio is not None) and (price_ratio < price_ratio_limit)
    condition_group1 = (med_dif is not None) and (med_dif < med_dif_10)
    condition_group2 = (med_dif is not None) and (med_dif < med_dif_20)

    if rural_ave == Decimal("0"):
        result = "0"
    elif same_shipping is not None and same_shipping == same_0 and meets_rural_condition:
        result = "1"
    elif (
        same_shipping is not None and same_shipping < same_10
        and meets_rural_condition and condition_group1
    ):
        result = "10"
    elif (
        same_shipping is not None
        and same_shipping < same_20
        and meets_rural_condition
        and meets_price_ratio
        and condition_group2
    ):
        result = "20"
    elif (
        same_shipping is not None and same_shipping < same_30
        and meets_rural_condition and meets_price_ratio
    ):
        result = "Extra2"
    else:
        if same_shipping is not None and same_shipping < same_50:
            result = "Extra3"
        elif same_shipping is not None and same_shipping < same_100:
            result = "Extra4"
        else:
            result = "Extra5"

    return str(result), price_ratio




def compute_cubic_weight(
    weight: Optional[float],
    cbm: Optional[float],
    cfg: Optional[Mapping[str, any]] = None,
    *,
    sku_code: Optional[str] = None,
) -> Optional[Decimal]:
    """
    CubicWeight：若 weight 或 CBM 为空 → null；
    否则若 weight > (CBM*250 - 1) → null；
    否则 CubicWeight = round(CBM*250, 2)。:contentReference[oaicite:15]{index=15}
    """
    w = _d(weight); c = _d(cbm)
    if w is None or c is None: return None

    factor = _cfgD(cfg, "cubic_factor", 250.0)
    headroom = _cfgD(cfg, "cubic_headroom", 1.0)
    if w > (c * factor - headroom):
        return None
    raw_cubic_weight = c * factor

    # todo check 保留2位小数
    # return _round(raw_cubic_weight, "0.01")
    
    # 使用 Banker's Rounding（四舍六入五留双）到 2 位小数
    return raw_cubic_weight.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)



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
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    
    st = (shipping_type or "").strip()
    st_lower = st.lower()
    is_extra = any(tag in st_lower for tag in ("extra3", "extra4", "extra5"))
    if not is_extra:
        return None

    w = _d(weight) or Decimal("0")
    cw = cubic_weight or Decimal("0")
    sm = shipping_med or Decimal("0")

    max_weight = max(w, cw)

    # 若 MaxWeight 或 ShippingMed 为 0
    divisor = _cfgD(cfg, "weight_calc_divisor", 1.5)
    tolerance = _cfgD(cfg, "weight_tolerance_ratio", 0.15)

    if max_weight == 0 or sm == 0:
        result = (sm / divisor) if sm != 0 else None
        return None if (result is None or result == 0) else _round(result, "0.01")

    calc_weight = sm / divisor
    # 避免除 0，上面已保证 max_weight > 0
    ratio_diff = (calc_weight - max_weight).copy_abs() / max_weight

    result = max_weight if ratio_diff <= tolerance else calc_weight
    if result == 0:
        return None
    
    # todo check 不保留2位小数
    return result
    # return _round(result, "0.01")




# --------- 价格计算 ----------
"""
生效价格：默认使用特价；若无特价则回落到常规价。
若特价结束日期早于或等于“明天”（本地时区），则提前回落到常规价。
"""
def compute_selling_price(
    price: Optional[float],
    special_price: Optional[float],
    special_price_end_date: Optional[Any] = None,
) -> Optional[Decimal]:
    sp = _d(special_price)
    rg = _d(price)

    if sp is None:
        return rg

    end_date: Optional[date] = None
    if special_price_end_date:
        if isinstance(special_price_end_date, datetime):
            dt = special_price_end_date
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_FREIGHT_TZ)
            end_date = dt.astimezone(_FREIGHT_TZ).date()
        elif isinstance(special_price_end_date, date):
            end_date = special_price_end_date
        else:
            try:
                end_date = datetime.strptime(str(special_price_end_date)[:10], "%Y-%m-%d").date()
            except Exception:
                end_date = None

    if end_date:
        today = datetime.now(_FREIGHT_TZ).date()
        if end_date <= (today + timedelta(days=1)):
            return rg
    
    # test使用
    # if end_date:
    #     now = datetime.now(_FREIGHT_TZ)
    #     today = now.date()
    #     if end_date < today:
    #         return rg
    #     if end_date == today and now.hour >= 23:
    #         return rg

    return sp if sp is not None else rg


def compute_shopify_price(
    selling_price: Optional[Decimal],
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    """
    Shopify Price：Selling Price < 25 用 1.26，否则 1.22；保留两位小数。
    这个就是用DSZ配置的shopify规则计算的
    """
    if selling_price is None: return None
    threshold = _cfgD(cfg, "shopify_threshold", 25.0)
    mult1 = _cfgD(cfg, "shopify_config1", 1.26)
    mult2 = _cfgD(cfg, "shopify_config2", 1.22)
    mult = mult1 if selling_price < threshold else mult2
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
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    
    if selling_price is None: return None
    vic = _d(vic_m) or Decimal("0")
    med_m = shipping_med or Decimal("0")
    w_as = weighted_ave_s or Decimal("0")
    high_denom = _cfgD(cfg, "kogan_au_normal_high_denom", 0.82)
    low_denom = _cfgD(cfg, "kogan_au_normal_low_denom", 0.79)
    extra5_discount = _cfgD(cfg, "kogan_au_extra5_discount", 0.969)
    vic_half_factor = _cfgD(cfg, "kogan_au_vic_half_factor", 0.5)
    # todo 也是用DSZ配置的shopify规则计算的
    threshold = _cfgD(cfg, "shopify_threshold", 25.0)

    st = str(shipping_type)
    if st == "Extra2":
        base = (selling_price + w_as) / high_denom
    elif st in ("Extra3", "Extra4"):
        base = (selling_price / high_denom) if vic == 0 else (selling_price + vic * vic_half_factor) / high_denom
    elif st == "Extra5":
        base = ((selling_price / high_denom) if vic == 0 else (selling_price + vic * vic_half_factor) / high_denom) / extra5_discount
    else:
        # 普通：<25 用 0.79，否则 0.82
        denom = low_denom if selling_price < threshold else high_denom
        base = (selling_price + med_m) / denom

    return _round(base, "0.01")


def compute_k1_price(
    kogan_au_price: Optional[Decimal],
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    """
    K1 Price：若 Kogan AUPrice > 66.7 → *0.969；否则减 2.01
    """
    if kogan_au_price is None: return None
    threshold = _cfgD(cfg, "k1_threshold", 66.7)
    multiplier = _cfgD(cfg, "k1_discount_multiplier", 0.969)
    minus = _cfgD(cfg, "k1_otherwise_minus", 2.01)
    if kogan_au_price > threshold:
        return _round(kogan_au_price * multiplier, "0.01")
    return kogan_au_price - Decimal(str(minus))


def compute_kogan_nz_price(
    selling_price: Optional[Decimal],
    nz_cost: Optional[float],
    cfg: Optional[Mapping[str, any]] = None,
) -> Optional[Decimal]:
    """
    Kogan NZPrice：NZ==9999 → null；否则 round((Selling + NZ)/(1-0.08-0.12)/0.9, 2)
    """
    if selling_price is None: return None
    nz = _d(nz_cost)
    service_no = _cfgD(cfg, "kogan_nz_service_no", 9999)
    if nz is None or nz == service_no:  # 9999 表示不送
        return None
    config1 = _cfgD(cfg, "kogan_nz_config1", 0.08)
    config2 = _cfgD(cfg, "kogan_nz_config2", 0.12)
    config3 = _cfgD(cfg, "kogan_nz_config3", 0.90)
    denom = Decimal("1") - config1 - config2
    if denom == 0 or config3 == 0:
        return None
    return _round((selling_price + nz) / denom / config3, "0.01")




# --------- 顶层：一次性计算一行 SKU 的全部变量 ----------
def compute_all(i: FreightInputs, 
                cfg: Optional[Mapping[str, any]] = None,
                *,
                sku_code: Optional[str] = None) -> FreightOutputs:

    fr = i.state_freight or {}

    selling_price = compute_selling_price(i.price, i.special_price, i.special_price_end_date)         # 生效价格
    adjust = compute_adjust(selling_price, cfg=cfg)                                                   # 低价调整

    same_shipping = compute_same_shipping(fr)
    shipping_ave = compute_shipping_ave(fr)
    shipping_ave_m = compute_m_shipping_ave(fr)
    shipping_ave_r = compute_r_shipping_ave(fr)
    shipping_med = compute_shipping_med(fr)

    remote_check = compute_remote_check(fr, cfg=cfg)
    rural_ave = compute_rural_ave(remote_check, fr, shipping_ave)
    weighted_ave_s = compute_weighted_ave_s(remote_check, shipping_ave, rural_ave, cfg=cfg)
    shipping_med_dif = compute_shipping_med_dif(fr, shipping_med)
    cubic_weight = compute_cubic_weight(i.weight, i.cbm, cfg=cfg, sku_code=sku_code)

    shipping_type, price_ratio_val = compute_shipping_type(
        same_shipping, rural_ave, shipping_med_dif, remote_check, i.price, cfg=cfg
    )

    weight = compute_weight(
        shipping_type=shipping_type,
        weight=i.weight,
        cubic_weight=cubic_weight,
        shipping_med=shipping_med,
        cfg=cfg,
    )

    shopify_price = compute_shopify_price(selling_price, cfg=cfg)
    kogan_au_price = compute_kogan_au_price(selling_price, shipping_type, fr.get("VIC_M"), shipping_med, weighted_ave_s, cfg=cfg)
    kogan_k1_price = compute_k1_price(kogan_au_price, cfg=cfg)
    kogan_nz_price = compute_kogan_nz_price(selling_price, fr.get("NZ"), cfg=cfg)
    price_ratio = (
        price_ratio_val if isinstance(price_ratio_val, Decimal) else _d(price_ratio_val)
    )


    adjust_q = _quantize(adjust, _Q_CENTS)
    same_shipping_q = _quantize(same_shipping, _Q_CENTS)
    shipping_ave_q = _quantize(shipping_ave, _Q_CENTS)
    shipping_ave_m_q = _quantize(shipping_ave_m, _Q_CENTS)
    shipping_ave_r_q = _quantize(shipping_ave_r, _Q_CENTS)
    shipping_med_q = _quantize(shipping_med, _Q_CENTS)
    rural_ave_q = _quantize(rural_ave, _Q_CENTS)
    weighted_ave_s_q = _quantize(weighted_ave_s, _Q_CENTS)
    shipping_med_dif_q = _quantize(shipping_med_dif, _Q_CENTS)
    cubic_weight_q = _quantize(cubic_weight, _Q_THOUSAND)
    weight_q = _quantize(weight, _Q_CENTS)
    price_ratio_q = _quantize(price_ratio, _Q_RATIO)
    selling_price_q = _quantize(selling_price, _Q_CENTS)
    shopify_price_q = _quantize(shopify_price, _Q_CENTS)
    kogan_au_price_q = _quantize(kogan_au_price, _Q_CENTS)
    kogan_k1_price_q = _quantize(kogan_k1_price, _Q_CENTS)
    kogan_nz_price_q = _quantize(kogan_nz_price, _Q_CENTS)

    return FreightOutputs(
        adjust=adjust_q,
        same_shipping=same_shipping_q,
        shipping_ave=shipping_ave_q,
        shipping_ave_m=shipping_ave_m_q,
        shipping_ave_r=shipping_ave_r_q,
        shipping_med=shipping_med_q,
        remote_check=remote_check,
        rural_ave=rural_ave_q,
        weighted_ave_s=weighted_ave_s_q,
        shipping_med_dif=shipping_med_dif_q,
        cubic_weight=cubic_weight_q,
        shipping_type=shipping_type,
        weight=weight_q,       # ← 新增字段返回
        price_ratio=price_ratio_q,
        selling_price=selling_price_q,
        shopify_price=shopify_price_q,
        kogan_au_price=kogan_au_price_q,
        kogan_k1_price=kogan_k1_price_q,
        kogan_nz_price=kogan_nz_price_q,
    )
