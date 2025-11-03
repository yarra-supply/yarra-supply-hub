from __future__ import annotations

from typing import Any, Dict, Mapping

from app.integrations.shopify.payload_utils import normalize_shopify_price



"""
    将 Shopify 侧的增量信息（variant id / price / tags）补充到标准化快照里。
    snapshot 会就地修改。
"""
def enrich_shopify_snapshot(
    snapshot: Dict[str, Any],
    sku: str,
    variant_map: Mapping[str, str],
    chunk_payload: Mapping[str, Dict[str, Any]],
) -> None:
    
    variant_id = variant_map.get(sku)
    if variant_id:
        snapshot["shopify_variant_id"] = variant_id

    payload = chunk_payload.get(sku) or {}
    if not payload:
        return

    price = payload.get("shopify_price")

    if price is not None:
        if hasattr(price, "quantize"):
            snapshot["shopify_price"] = price
        else:
            normalized_price = normalize_shopify_price(price)
            if normalized_price is not None:
                snapshot["shopify_price"] = normalized_price

    if "product_tags" in payload:
        snapshot["product_tags"] = payload.get("product_tags") or []
    elif "tags" in payload:
        snapshot["product_tags"] = payload.get("tags") or []
