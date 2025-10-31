
"""
低层 HTTP 客户端：鉴权/限流/重试/401刷新
  - 负责 /auth 换取 JWT、自动刷新（401）；
  - 基于简单节流（X req/min）与指数退避（429/5xx/网络异常）；
  - 提供 get_json/post_json 两个入口，不关心业务字段结构。
"""

from __future__ import annotations
import json, logging,random, time, math, requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from app.core.config import settings
from app.integrations.dsz.errors import (
    DSZAuthError, DSZClientError, DSZServerError, DSZRateLimitError, DSZPayloadError
)
from app.infrastructure.ratelimit.redis_token_bucket import RedisTokenBucketLimiter

logger = logging.getLogger(__name__)

try:
    import redis  # type: ignore
except Exception:  # 没装也能跑，会降级
    redis = None
    logger.warning("redis not available, some features may be disabled")



@dataclass
class _Token:
    value: str
    expires_at: datetime  # UTC

def _now_utc() -> datetime:
    """返回当前 UTC 时间，用于比较 token 过期。"""
    return datetime.now(timezone.utc)



class DSZHttpClient:
    """Dropshipzone Retailer API 的低层 HTTP 客户端：负责鉴权、限流与重试。"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        connect_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        rate_limit_per_min: Optional[int] = None,
        token_ttl_fallback_sec: Optional[int] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        """初始化客户端，允许覆盖基础配置以便测试或多账号场景。"""
        self.base_url = (base_url or str(settings.DSZ_BASE_URL)).rstrip("/") + "/"
        self.email = email or settings.DSZ_API_EMAIL
        self.password = password or settings.DSZ_API_PASSWORD
        self.connect_timeout = connect_timeout or settings.DSZ_CONNECT_TIMEOUT
        self.read_timeout = read_timeout or settings.DSZ_READ_TIMEOUT
        self.rate_limit_per_min = rate_limit_per_min or settings.DSZ_RATE_LIMIT_PER_MIN
        self.token_ttl_fallback_sec = token_ttl_fallback_sec or settings.DSZ_TOKEN_TTL_SEC

        self._session = session or requests.Session()
        self._token: Optional[_Token] = None
        self._last_request_ts: float = 0.0
        # 用全局限流：Redis 构造一个令牌桶, 通过名字组合出Redis，可以不同机器公用
        self._global_limiter = RedisTokenBucketLimiter.from_settings(vendor="dsz", account=self.email)




    # ---------- Public ----------
    # test ✅
    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Any:
        """发送 GET 请求并返回解析后的 JSON，附带鉴权/重试/限流。"""
        resp = self._request("GET", path, params=params, **kwargs)
        return self._as_json(resp)
    
    # 不需要
    def post_json(self, path: str, json_body: Optional[Dict[str, Any]] = None, **kwargs) -> Any:
        """发送 POST 请求并返回解析后的 JSON，附带鉴权/重试/限流。"""
        resp = self._request("POST", path, json=json_body, **kwargs)
        return self._as_json(resp)
    

    # 可选：get_zone_rates 的便捷包装（上层也可以直接用 post_json）
    def get_zone_rates(self, skus: list[str], page_no: int = 1, limit: int = 160) -> Any:
        """
        便捷方法：调用 /v2/get_zone_rates，返回原始 JSON。
        """
        path = getattr(settings, "DSZ_ZONE_RATES_ENDPOINT", "/v2/get_zone_rates")
        body = {"skus": ",".join(skus) if skus else "", "page_no": page_no, "limit": limit}
        return self.post_json(path, json_body=body)


    # test ✅
    # ---------- Internals ----------
    def _as_json(self, resp: requests.Response) -> Any:
        """尝试解析响应 JSON；若失败则截取文本并抛 DSZPayloadError。"""
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "application/json" not in ctype:
            logger.warning("DSZ non-JSON response Content-Type=%s", ctype)
        try:
            return resp.json()
        except Exception as e:  
            text = (resp.text or "")[:500]  # 截断，避免日志过大  
            raise DSZPayloadError(f"non-JSON response (status={resp.status_code}): {text}") from e 


    # test ✅
    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """执行一次底层 HTTP 调用，负责 token、限流、重试与状态码处理。"""

        # 1) 确保token: 没有 token 或已过期（按 UTC 比较）就去 /auth 重新取；过期时间也按 UTC 存
        self._ensure_token()

        # 2) 构造请求
        url = urljoin(self.base_url, path.lstrip("/"))
        headers = kwargs.pop("headers", {}) or {}
        headers.setdefault("Accept", "application/json")
        # 文档要求即使 GET 也带 Content-Type: application/json
        headers.setdefault("Content-Type", "application/json") 
        # DSZ 文档使用 jwt 前缀
        headers["Authorization"] = f"jwt {self._token.value}"   


        # 设定超时
        timeout = kwargs.pop("timeout", (self.connect_timeout, self.read_timeout))

        # 3） 简单节流：X 次/分钟 → 每次请求间隔 ≈ 60/X 秒
        self._respect_rate_limit()

        # 4）重试查询DSZ接口
        max_attempts = 3
        already_refreshed = False

        for attempt in range(1, max_attempts + 1):
            try:
                resp = self._session.request(method, url, headers=headers, timeout=timeout, **kwargs)

                # 打印 resp 的关键信息，避免过长输出（截断 body 到 1000 字符）
                try:
                    info_msg = f"DSZ response: {method} {url} -> {resp.status_code}"
                    # logger.debug(info_msg)
                    # logger.debug("DSZ response headers: %s", dict(resp.headers))
                    body_text = resp.text or ""
                    try:
                        parsed = resp.json()
                        pretty = json.dumps(parsed, ensure_ascii=False, indent=2)
                    except Exception:
                        pretty = body_text
                    logger.debug("DSZ response body: %s", pretty)

                    # Also print to stdout
                    # print(info_msg)
                    # print("DSZ response headers:", dict(resp.headers))
                    # print("DSZ response body (truncated 1000 chars):", pretty)
                except Exception:
                    logger.exception("Failed to log DSZ response")

            except requests.RequestException as e:
                # error1: 连接/超时等异常：指数退避
                self._sleep_backoff(attempt)
                if attempt == max_attempts:
                    raise DSZClientError(f"request error: {e}") from e
                continue

            # 返回单调递增的时钟秒数：每次成功发出 HTTP 请求后记录“上一次请求的发出时刻”
            self._last_request_ts = time.monotonic()

            # error2: 401 未授权：只做一次自动刷新
            if resp.status_code == 401 and not already_refreshed:
                # token 无效/过期 → 刷新一次并重放
                logger.info("DSZ 401 received, refreshing token once.")
                self._authenticate(force=True)
                headers["Authorization"] = f"jwt {self._token.value}"  # type: ignore[union-attr]
                already_refreshed = True
                continue

            # 429 限流：指数退避后重试；用尽重试则抛 DSZRateLimitError
            if resp.status_code == 429:
                self._sleep_backoff(attempt)
                if attempt == max_attempts:
                    raise DSZRateLimitError(f"429 after retries: {resp.text}")
                continue

            # 5xx 服务端错误：指数退避后重试；用尽重试则抛 DSZServerError
            if resp.status_code >= 500:
                self._sleep_backoff(attempt)
                if attempt == max_attempts:
                    raise DSZServerError(f"{resp.status_code} after retries: {resp.text}")
                continue

            # —— 其它 4xx 统一转为 DSZClientError，便于上层处理 ——  
            if 400 <= resp.status_code < 500 and resp.status_code not in (401, 429):
                snippet = (resp.text or "")[:300]
                raise DSZClientError(f"{resp.status_code} client error: {snippet}")

            # 其他非 2xx，交给上层处理
            resp.raise_for_status()

            return resp    # 成功

        # 理论上不会走到这里
        raise DSZClientError("unreachable retry loop")



    # ---------- Helpers ----------
    # todo test
    def _respect_rate_limit(self) -> None:
        """优先使用 Redis 令牌桶限流；不可用时退回进程内节流。"""
        # --- 全局限流 --
        limiter = getattr(self, "_global_limiter", None)  # 如果实例上有 _global_limiter 属性，就取出来
        if limiter is not None:
            try:
                for _ in range(20):     # 尝试最多 20 次抢 token ~20 * 5s = 100s（通常远小于此）
                    allowed, wait_ms = limiter.acquire_once()    # wait_ms 令牌桶算法内部计算出来的
                    if allowed:  # 抢到了一个 token，可以立即发请求
                        return
                    
                    time.sleep(max(0.001, (wait_ms or 1000) / 1000.0))

                # 超过 20 次仍未拿到：最后再 sleep 一会儿作为退避，然后返回让上层重试节奏接管
                time.sleep(1.0)
            except Exception as e:
                logger.warning("Global rate-limit disabled due to Redis error: %s; falling back to process-local.", e)
                
        # --- 进程内节流（兜底） ---
        if not self.rate_limit_per_min or self.rate_limit_per_min <= 0:
            return
        interval = 60.0 / float(self.rate_limit_per_min)
        now = time.monotonic()
        delta = now - self._last_request_ts
        if delta < interval:
            time.sleep(interval - delta)


    # 指数退避：上限 60 秒，加上 0~25% 抖动。例：2s, 4s, 8s, 16s, 32s, 60s
    # todo test
    def _sleep_backoff(self, attempt: int) -> None:
        """指数退避等待，加入 0~25%% 抖动，平衡重试压力。"""
        base = min(2 ** attempt, 60)  # 2,4,8,16,32,60...
        jitter = random.uniform(0, 0.25 * base)
        time.sleep(base + jitter)


    # 确认token生效
    # test ✅
    def _ensure_token(self) -> None:
        """在发请求前确保 token 存在且未过期。"""
        if self._token is None or _now_utc() >= self._token.expires_at:
            self._authenticate(force=True)


    # 更新 Authorization
    # test ✅
    def _authenticate(self, force: bool = False) -> None:
        """调用 /auth 获取新 token，更新 Authorization 缓存。"""
        if self._token and not force and _now_utc() < self._token.expires_at:
            return

        url = urljoin(self.base_url, "auth")
        body = {"email": self.email, "password": self.password}
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        try:
            resp = self._session.post(url, json=body, headers=headers, timeout=(self.connect_timeout, self.read_timeout))
        except requests.RequestException as e:
            raise DSZAuthError(f"/auth request error: {e}") from e

        if resp.status_code >= 400:
            raise DSZAuthError(f"/auth failed: {resp.status_code} {resp.text}")

        try:
            data = resp.json()
        except Exception as e:
            raise DSZAuthError(f"/auth non-JSON response: {e}") from e

        token = self._extract_token_value(data)
        if not token:
            raise DSZAuthError(f"/auth missing token in response: {data}")

        expires_at = self._extract_token_expiry(data)
        self._token = _Token(value=token, expires_at=expires_at)
        logger.info("DSZ authenticated; token expires at %s", expires_at.isoformat())


    def _extract_token_value(self, data: Dict[str, Any]) -> Optional[str]:
        """从鉴权响应中提取 token 字符串，兼容多种键名。"""
        for key in ("token", "access_token", "accessToken", "jwt"):
            v = data.get(key)
            if isinstance(v, str) and v:
                return v
        return None


    # todo test
    def _extract_token_expiry(self, data: Dict[str, Any]) -> datetime:
        """推断 token 过期时间，优先读取 exp/expires_in，否则使用配置兜底。"""
        now = _now_utc()
        exp = data.get("exp")
        if isinstance(exp, (int, float)):
            # 兼容毫秒
            if exp > 10_000_000_000:
                exp = exp / 1000.0
            return datetime.fromtimestamp(float(exp), tz=timezone.utc)

        expires_in = data.get("expires_in")
        if isinstance(expires_in, (int, float)) and expires_in > 0:
            return now + timedelta(seconds=float(expires_in))

        return now + timedelta(seconds=self.token_ttl_fallback_sec)
