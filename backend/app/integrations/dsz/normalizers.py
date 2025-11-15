

"""
领域映射（纯函数）：将 DSZ 返回的产品字典转换为你项目内部字段。
"""
from __future__ import annotations
from typing import Any, Dict, Optional, List
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
import re



"""
DSZ → SkuInfo 精确映射
    - 输入：DSZ V2/GetProducts 返回的一条产品字典（仅使用官方字段名）
    - 输出：对齐 SkuInfo 的字段字典（只包含非 None 值）
"""
def normalize_dsz_product(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入: DSZ 原始产品字典
    输出: 内部统一字段（示例字段，可按业务继续扩展）
    """

    out: Dict[str, Any] = {}

    out["sku_code"] = str(raw.get("sku") or "").strip()

    brand = raw.get("brand")
    if isinstance(brand, str):
        brand = brand.strip()
        brand_lower = brand.lower()
        fallback_keywords = ("unbranded", "does not apply", "na", "genetic")
        if any(keyword in brand_lower for keyword in fallback_keywords):
            brand = ""
        elif _has_garbled_characters(brand):
            brand = ""
    if not brand:
        brand = "Yarra Supply"
    out["brand"] = brand

    out["supplier"] = raw.get("vendor_id")

    out["stock_qty"] = _to_int(raw.get("stock_qty"))

    out["ean_code"] = str(raw.get("eancode") or "").strip()

    # 价格相关
    out["price"] = _to_decimal(raw.get("price"))

    # rrp直接使用 dsz 
    out["rrp_price"] = _to_decimal(raw.get("RrpPrice"))

    out["special_price"] = _to_decimal(raw.get("special_price"))

    out["special_price_end_date"] = _parse_date(raw.get("special_price_end_date"))

    # --- 尺寸/重量（统一：cm/kg，保留 3 位小数） ---
    length = _to_decimal(raw.get("length"), q="0.001")
    width  = _to_decimal(raw.get("width"), q="0.001")
    height = _to_decimal(raw.get("height"), q="0.001")
    weight = _to_decimal(raw.get("weight"), q="0.001")
    cbm    = _to_decimal(raw.get("cbm"), q="0.0001")
    out["length"] = length
    out["width"] = width
    out["height"] = height
    out["weight"] = weight
    out["cbm"] = cbm

    # --- 运费字段（17 项，对齐表里的命名） ---
    # v = _to_decimal(raw.get("ACT"))
    # if v is not None: out["freight_act"] = v
    # v = _to_decimal(raw.get("NSW_M"))
    # if v is not None: out["freight_nsw_m"] = v
    # v = _to_decimal(raw.get("NSW_R"))
    # if v is not None: out["freight_nsw_r"] = v
    # v = _to_decimal(raw.get("QLD_M"))
    # if v is not None: out["freight_qld_m"] = v
    # v = _to_decimal(raw.get("QLD_R"))
    # if v is not None: out["freight_qld_r"] = v
    # v = _to_decimal(raw.get("SA_M"))
    # if v is not None: out["freight_sa_m"] = v
    # v = _to_decimal(raw.get("SA_R"))
    # if v is not None: out["freight_sa_r"] = v
    # v = _to_decimal(raw.get("TAS_M"))
    # if v is not None: out["freight_tas_m"] = v
    # v = _to_decimal(raw.get("TAS_R"))
    # if v is not None: out["freight_tas_r"] = v
    # v = _to_decimal(raw.get("VIC_M"))
    # if v is not None: out["freight_vic_m"] = v
    # v = _to_decimal(raw.get("VIC_R"))
    # if v is not None: out["freight_vic_r"] = v
    # v = _to_decimal(raw.get("WA_M"))
    # if v is not None: out["freight_wa_m"] = v
    # v = _to_decimal(raw.get("WA_R"))
    # if v is not None: out["freight_wa_r"] = v
    # v = _to_decimal(raw.get("NT_M"))
    # if v is not None: out["freight_nt_m"] = v
    # v = _to_decimal(raw.get("NT_R"))
    # if v is not None: out["freight_nt_r"] = v
    # v = _to_decimal(raw.get("NZ"))
    # if v is not None: out["freight_nz"] = v
    # remote = _to_decimal(raw.get("REMOTE"))
    # if remote is not None:
    #     out["remote"] = remote


    #  新接口：/v2/get_zone_rates 返回的 "standard"（小写键）
    std = raw.get("_zone_standard") or raw.get("standard")
    if isinstance(std, dict):
        # 小写字段映射 → 表字段
        mapping = {
            "act": "freight_act",
            "nsw_m": "freight_nsw_m",
            "nsw_r": "freight_nsw_r",
            "qld_m": "freight_qld_m",
            "qld_r": "freight_qld_r",
            "sa_m": "freight_sa_m",
            "sa_r": "freight_sa_r",
            "tas_m": "freight_tas_m",
            "tas_r": "freight_tas_r",
            "vic_m": "freight_vic_m",
            "vic_r": "freight_vic_r",
            "wa_m": "freight_wa_m",
            "wa_r": "freight_wa_r",
            "nt_m": "freight_nt_m",
            "nt_r": "freight_nt_r",
            "nz": "freight_nz",
            "remote": "remote",
        }
        for k, out_key in mapping.items():
            val = _to_decimal(std.get(k))
            out[out_key] = val


    return out




# ============= tool function ===============

def _parse_date(val) -> Optional[date]:
    """
    兼容 DSZ 可能返回的几种形式：
    - 'YYYY-MM-DD' 'YYYY/MM/DD'
    - 带时间的 ISO 字符串
    - datetime / date 对象
    """
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()

    s = str(val).strip()
    if not s:
        return None

    # 仅取日期段
    m = re.match(r"^(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    # 尝试 ISO
    try:
        return datetime.fromisoformat(s[:19]).date()
    except Exception:
        return None
    

def _to_decimal(val, q: str = "0.01") -> Optional[Decimal]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        d = Decimal(s)
        return d.quantize(Decimal(q))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_float(val, ndigits: Optional[int] = None) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        f = float(s)
        if ndigits is not None:
            f = round(f, ndigits)
        return f
    except (ValueError, TypeError):
        return None


def _to_int(val) -> Optional[int]:
    f = _to_float(val)
    return int(f) if f is not None else None


def _has_garbled_characters(value: str) -> bool:
    """
    简单判定品牌名是否含非 ASCII 可打印字符（如 “à” 等乱码）。
    """
    for ch in value:
        code = ord(ch)
        if code < 32 or code > 126:
            return True
    return False
