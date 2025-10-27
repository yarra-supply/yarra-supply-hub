
"""
对外统一入口（Public Surface）：
- 从这里 import 需要的类/函数，内部实现可自由演进。
- 提供 DSZClient 别名兼容历史调用习惯。
"""

from .dsz_products import (
    DSZProductsAPI,
    get_products_by_skus,
    get_products_by_skus_with_stats,
)

from .normalizers import normalize_dsz_product

from .errors import (
    DSZError, DSZAuthError, DSZClientError, DSZServerError, DSZRateLimitError, DSZPayloadError
)


__all__ = [
    "DSZProductsAPI",
    "get_products_by_skus", "get_products_by_skus_with_stats",
    "normalize_dsz_product",
    "DSZError", "DSZAuthError", "DSZClientError", "DSZServerError", "DSZRateLimitError", "DSZPayloadError",
]
