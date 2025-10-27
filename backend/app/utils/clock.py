
from __future__ import annotations
from datetime import datetime, timezone

def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)  # 与 DB naive UTC 对齐
