
from __future__ import annotations

def calc_next_delay(attempts: int, base_seconds: int = 10, max_seconds: int = 1800) -> int:
    """
    指数退避：1次失败→base，之后翻倍，直到 max_seconds。
    attempts: 已尝试次数（即将写入的次数）
    """
    attempts = max(1, attempts)
    delay = base_seconds * (2 ** (attempts - 1))
    return min(max_seconds, delay)
