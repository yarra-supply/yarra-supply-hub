

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
    if not brand:
        brand = "Yarra Supply"
    out["brand"] = brand

    out["supplier"] = "Yarra Supply"

    out["stock_qty"] = _to_int(raw.get("stock_qty"))

    ean = str(raw.get("eancode") or "").strip()
    if ean:
        out["ean_code"] = ean

    # 价格相关
    price = _to_decimal(raw.get("price"))
    if price is not None:
        out["price"] = price

    rrp = _to_decimal(raw.get("RrpPrice"))
    if rrp is not None:
        out["rrp_price"] = rrp

    sp = _to_decimal(raw.get("special_price"))
    if sp is not None:
        out["special_price"] = sp

    sp_end = _parse_date(raw.get("special_price_end_date"))
    if sp_end:
        out["special_price_end_date"] = sp_end

    # --- 尺寸/重量（统一：cm/kg，保留 3 位小数） ---
    length = _to_decimal(raw.get("length"), q="0.001")
    width  = _to_decimal(raw.get("width"), q="0.001")
    height = _to_decimal(raw.get("height"), q="0.001")
    weight = _to_decimal(raw.get("weight"), q="0.001")
    cbm    = _to_decimal(raw.get("cbm"), q="0.0001")
    if length is not None: out["length"] = length
    if width  is not None: out["width"]  = width
    if height is not None: out["height"] = height
    if weight is not None: out["weight"] = weight
    if cbm    is not None: out["cbm"]    = cbm

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
            if val is not None:
                out[out_key] = val


    # 去掉 None，避免污染 upsert
    return {k: v for k, v in out.items() if v is not None}




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
