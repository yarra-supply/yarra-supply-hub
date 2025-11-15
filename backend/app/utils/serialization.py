from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
import math
import uuid


def format_product_tags(value: Any) -> Optional[str]:
    """
    Normalize product tags to a comma-separated string.
    """
    if value is None:
        return None
    if isinstance(value, list):
        tokens = [str(v).strip() for v in value if v is not None and str(v).strip()]
        return ",".join(tokens)
    if isinstance(value, dict):
        # JSON dump but keep ASCII? existing behavior used ensure_ascii False
        import json

        return json.dumps(value, ensure_ascii=False)
    return str(value)


def to_jsonable(value: Any):
    """
    Recursively convert arbitrary Python values into JSON-serializable primitives.
    """
    if isinstance(value, Decimal):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value
