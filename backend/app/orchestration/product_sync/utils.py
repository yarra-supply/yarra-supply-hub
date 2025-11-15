from __future__ import annotations

from typing import Any, Dict, List, Tuple

from app.repository.product_repo import SYNC_FIELDS
from app.utils.serialization import to_jsonable


def diff_snapshot(old: Dict[str, Any] | None, new: Dict[str, Any]) -> set[str]:
    base = old or {}
    changed: set[str] = set()
    for field in SYNC_FIELDS:
        if base.get(field) != new.get(field):
            changed.add(field)
    return changed


def build_candidate_rows(run_id: str, tuples: List[Tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    for sku, new_fields in tuples or []:
        if not new_fields:
            continue
        key = (run_id, sku)
        if key in seen:
            continue
        seen.add(key)
        change_mask = {str(k): True for k in new_fields.keys()}
        if not change_mask:
            continue
        rows.append(
            {
                "run_id": run_id,
                "sku_code": sku,
                "change_mask": change_mask,
                "new_snapshot": to_jsonable(new_fields),
                "change_count": len(change_mask),
            }
        )
    return rows
