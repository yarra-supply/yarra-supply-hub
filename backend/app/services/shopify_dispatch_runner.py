
from __future__ import annotations
from typing import Optional, Callable
from sqlalchemy.orm import Session


# 尝试复用你现有的派发实现（任一存在即可）
_dispatch_one: Optional[Callable] = None
_dispatch_many: Optional[Callable] = None
_apply_metafields: Optional[Callable] = None


try:
    from app.services.shopify_dispatch import dispatch_job as _dispatch_one  # type: ignore
except Exception:
    _dispatch_one = None
try:
    from app.services.shopify_dispatch import dispatch_jobs as _dispatch_many  # type: ignore
except Exception:
    _dispatch_many = None
try:
    from app.services.shopify_dispatch import apply_metafields as _apply_metafields  # type: ignore
except Exception:
    _apply_metafields = None



def run_one_job(job, db: Session) -> None:
    """
    调用实际 Shopify 执行：
      1) dispatch_job(job, db)
      2) dispatch_jobs([job], db)
      3) apply_metafields(sku_code, payload["metafields"])
    """
    if _dispatch_one:
        _dispatch_one(job, db)  # type: ignore
        return
    if _dispatch_many:
        _dispatch_many([job], db)  # type: ignore
        return
    if _apply_metafields and getattr(job, "op", None) == "metafieldsSet":
        payload = getattr(job, "payload", {}) or {}
        mf = payload.get("metafields") if isinstance(payload, dict) else None
        if not isinstance(mf, dict):
            raise ValueError("metafieldsSet payload invalid")
        sku = getattr(job, "sku_code", None)
        _apply_metafields(sku, mf)  # type: ignore
        return
    raise RuntimeError(
        "No dispatch function found. Provide one of: "
        "shopify_dispatch.dispatch_job(job, db) / dispatch_jobs(jobs, db) / apply_metafields(sku, metafields)"
    )
