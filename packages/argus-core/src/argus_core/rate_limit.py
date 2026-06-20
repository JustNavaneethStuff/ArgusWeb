"""Phase 2 scaffold: rate limiting."""

from __future__ import annotations

from abc import ABC, abstractmethod


class RateLimiter(ABC):
    @abstractmethod
    async def acquire(self, domain: str) -> None:
        """Block until a request token is available for the domain."""


class NoOpRateLimiter(RateLimiter):
    async def acquire(self, domain: str) -> None:
        return None


class RedisTokenBucketRateLimiter(RateLimiter):
    """Phase 2: Redis token bucket per domain."""

    def __init__(self, redis, tokens_per_second: float = 1.0, burst: int = 5) -> None:
        self._redis = redis
        self._tokens_per_second = tokens_per_second
        self._burst = burst

    async def acquire(self, domain: str) -> None:
        # Phase 2 implementation placeholder
        return None
