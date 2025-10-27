

"""""
   DSZ（Dropshipzone）集成层专用异常类型。
   将 HTTP/限流/服务端/载荷等错误与业务层解耦，便于上层统一处理。
"""

class DSZError(Exception):
    """Base for all DSZ errors."""

class DSZAuthError(DSZError):
    """Auth (/auth) failed or token invalid and cannot be refreshed."""

class DSZClientError(DSZError):
    """Network/client-side errors after retries."""

class DSZServerError(DSZError):
    """Server-side (5xx) errors after retries."""

class DSZRateLimitError(DSZError):
    """429 Too Many Requests not resolved after retries."""

class DSZPayloadError(DSZError):
    """Unexpected/invalid response payload shape or content."""
