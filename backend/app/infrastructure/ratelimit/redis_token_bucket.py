
# app/infrastructure/ratelimit/redis_token_bucket.py
from __future__ import annotations
import time, logging
from typing import Optional, Tuple

try:
    import redis  # type: ignore
except Exception:
    redis = None  # 允许上层优雅降级



"""
全局令牌桶限流（多进程/多机共享），单位：rpm。
    key: {prefix}:{env}:{vendor}:{account}:v2

    acquire_once() 原子步骤（Lua）：
      1) 用 Redis 服务器时间（TIME）计算补桶
      2) 若 tokens >= 1 则消耗 1 个并 allowed=1；否则返回需要等待的毫秒 wait_ms
      3) 持久化 tokens/ts，并设置 TTL（空闲自动清理）
"""
class RedisTokenBucketLimiter:
    
    LUA_SCRIPT = """
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_per_ms = tonumber(ARGV[2])
    local ttl_ms = tonumber(ARGV[3])

    -- [MODIFIED] 使用 Redis 服务器时间，避免多主机时钟偏差
    local t = redis.call('TIME')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

    -- 读取状态
    local data = redis.call('HMGET', key, 'tokens', 'ts')
    local tokens = tonumber(data[1])
    local ts = tonumber(data[2])

    if tokens == nil or ts == nil then
        tokens = capacity
        ts = now
    else
        local delta = now - ts
        if delta < 0 then delta = 0 end
        local filled = delta * refill_per_ms
        tokens = math.min(capacity, tokens + filled)
        ts = now
    end

    local allowed = 0
    local wait_ms = 0
    if tokens >= 1 then
        tokens = tokens - 1
        allowed = 1
    else
        -- 令牌不足：需要等待直到 tokens 回到 1
        wait_ms = math.ceil((1 - tokens) / refill_per_ms)
        if wait_ms < 0 then wait_ms = 0 end
    end

    -- [MODIFIED] HSET 替代 HMSET
    redis.call('HSET', key, 'tokens', tokens, 'ts', ts)
    if ttl_ms > 0 then
      redis.call('PEXPIRE', key, ttl_ms)
    end
    return {allowed, tokens, wait_ms}
    """


    def __init__(self, client, key: str, max_rpm: int, burst: int = 5, 
                 ttl_ms: int = 120000, max_wait_ms: Optional[int] = 5000):
        self.r = client
        self.key = key
        self.capacity = max(1, int(burst))
        self.refill_per_ms = float(max_rpm) / 60_000.0
        self.ttl_ms = int(ttl_ms)
        self.max_wait_ms = max_wait_ms 
        self._sha = self.r.script_load(self.LUA_SCRIPT)



    """
       从 settings 里自动读取限流开关/Redis URL/速率/桶容量/前缀/环境，构造 limiter。
    """
    @classmethod
    def from_settings(cls, *, vendor: str, account: str | None) -> RedisTokenBucketLimiter | None:
        from app.core.config import settings

        if not getattr(settings, "DSZ_GLOBAL_RL_ENABLED", False):
            return None
        
        url = getattr(settings, "DSZ_GLOBAL_RATE_LIMIT_REDIS_URL", None)
        if not url or redis is None:
            logging.getLogger(__name__).warning("Global RL disabled (no redis or url).")
            return None
        
        r = redis.from_url(url, decode_responses=True)
        prefix = getattr(settings, "DSZ_GLOBAL_RL_KEY_PREFIX", "dsz:rl")
        env = getattr(settings, "DSZ_ENV", "dev")
        acct = (account or "account").replace("@", "_at_")
        key = f"{prefix}:{env}:{vendor}:{acct}:v2"

        return cls(
            client=r,
            key=key,
            max_rpm=int(getattr(settings, "DSZ_GLOBAL_RL_MAX_RPM", 60)),
            burst=int(getattr(settings, "DSZ_GLOBAL_RL_BURST", 5)),
            ttl_ms=120000,
            max_wait_ms=int(getattr(settings, "DSZ_GLOBAL_RL_MAX_WAIT_MS", 5000)),  
        )
    

    """
        执行 Lua（带 NOSCRIPT 兜底重载）。
    """
    def _eval(self) -> Tuple[bool, int]:
        try:
            res = self.r.evalsha(
                self._sha, 1, self.key, self.capacity, self.refill_per_ms, self.ttl_ms  # [MODIFIED] 不再传 now_ms
            )
        except Exception as e:
            # [NEW] 兜底：Redis 重启后 evalsha 可能报 NOSCRIPT，这里重载脚本再试一次
            msg = str(e)
            if "NOSCRIPT" in msg or "noscript" in msg:
                self._sha = self.r.script_load(self.LUA_SCRIPT)
                res = self.r.evalsha(
                    self._sha, 1, self.key, self.capacity, self.refill_per_ms, self.ttl_ms
                )
            else:
                raise
        allowed = int(res[0]) == 1
        wait_ms = 0 if allowed else max(0, int(float(res[2])))
        if (self.max_wait_ms is not None) and (wait_ms > self.max_wait_ms):  # [NEW]
            wait_ms = self.max_wait_ms
        return allowed, wait_ms
    


    """
        尝试消费 1 个令牌；返回 (allowed, wait_ms)。
        - allowed=True：允许立即发请求
        - allowed=False：建议等待 wait_ms 毫秒后再试
    """
    def acquire_once(self) -> tuple[bool, int]:
        return self._eval()  
