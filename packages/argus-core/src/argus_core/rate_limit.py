"""Phase 2: per-domain rate limiting."""

from __future__ import annotations

import asyncio
import time

from abc import ABC, abstractmethod


class RateLimiter(ABC):
    @abstractmethod
    async def acquire(self, domain: str) -> None:
        """Block until a request token is available for the domain."""


class NoOpRateLimiter(RateLimiter):
    async def acquire(self, domain: str) -> None:
        return None


_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])

if tokens == nil then
  tokens = burst
  last = now
end

local elapsed = math.max(0, now - last)
tokens = math.min(burst, tokens + elapsed * rate)

if tokens < 1 then
  local wait = (1 - tokens) / rate
  return {0, wait}
end

tokens = tokens - 1
redis.call('HMSET', key, 'tokens', tokens, 'last', now)
redis.call('EXPIRE', key, math.ceil(burst / rate) + 60)
return {1, 0}
"""


class RedisTokenBucketRateLimiter(RateLimiter):
    """Redis token bucket per domain."""

    def __init__(
        self,
        redis,
        tokens_per_second: float = 1.0,
        burst: int = 5,
    ) -> None:
        self._redis = redis
        self._tokens_per_second = tokens_per_second
        self._burst = burst
        self._script = self._redis.register_script(_TOKEN_BUCKET_LUA)

    def _key(self, domain: str) -> str:
        return f"argus:ratelimit:{domain}"

    async def acquire(self, domain: str) -> None:
        while True:
            result = await self._script(
                keys=[self._key(domain)],
                args=[self._tokens_per_second, self._burst, time.time()],
            )
            allowed, wait_seconds = int(result[0]), float(result[1])
            if allowed:
                return
            await asyncio.sleep(max(wait_seconds, 0.01))
