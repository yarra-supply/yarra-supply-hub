from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple


def normalize_tags(value: Any) -> List[str]:
    """
    将 Shopify 返回的标签（通常为 list[str]）归一化为字符串列表。
    对于逗号分隔的字符串等异常形式也做兼容处理。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if isinstance(v, str) and str(v).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def normalize_shopify_price(value: Any) -> Decimal | None:
    """
    将 Shopify 变体上的 price 转换为 Decimal，失败则返回 None。
    """
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_sku_payload(payload: list[Any] | None) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """
    解析 scheduler 传入的分片 payload，返回：
      - 当前分片涉及的 SKU 列表
      - sku -> {variant_id, shopify_price, product_tags, ...} 的映射
    """
    skus: List[str] = []
    data_map: Dict[str, Dict[str, Any]] = {}
    if not payload:
        return skus, data_map

    for entry in payload:
        sku: str = ""
        variant_id: str | None = None
        raw_price = None
        has_price = False
        tags_in_payload = False
        tags_value: List[str] | None = None

        if isinstance(entry, dict):
            sku = str(entry.get("sku") or entry.get("sku_code") or "").strip()
            variant_id = entry.get("variant_id") or entry.get("shopify_variant_id")
            if "price" in entry or "shopify_price" in entry:
                raw_price = entry.get("shopify_price", entry.get("price"))
                has_price = True
            if "tags" in entry:
                tags_in_payload = True
                tags_value = normalize_tags(entry.get("tags"))
            elif "product_tags" in entry:
                tags_in_payload = True
                tags_value = normalize_tags(entry.get("product_tags"))
        elif isinstance(entry, (list, tuple)):
            if entry:
                sku = str(entry[0] or "").strip()
            if len(entry) > 1:
                variant_id = entry[1]
        else:
            sku = str(entry or "").strip()

        if not sku:
            continue

        skus.append(sku)
        data = data_map.setdefault(sku, {})
        if variant_id:
            variant_str = str(variant_id).strip()
            if variant_str:
                data["variant_id"] = variant_str
        if has_price:
            normalized_price = normalize_shopify_price(raw_price)
            if normalized_price is not None:
                data["shopify_price"] = normalized_price
        if tags_in_payload:
            data["product_tags"] = tags_value or []

    return skus, data_map
