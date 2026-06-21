from __future__ import annotations

import redis.asyncio as aioredis

from argus_core.proxy import NoOpProxyRotator, ProxyRotator, RoundRobinProxyRotator
from argus_core.rate_limit import NoOpRateLimiter, RateLimiter, RedisTokenBucketRateLimiter
from argus_core.retry import ExponentialBackoffRetry, RetryStrategy
from argus_core.settings import Settings


def build_rate_limiter(settings: Settings, redis: aioredis.Redis | None = None) -> RateLimiter:
    if redis is None:
        return NoOpRateLimiter()
    return RedisTokenBucketRateLimiter(
        redis,
        tokens_per_second=settings.rate_limit_tokens_per_second,
        burst=settings.rate_limit_burst,
    )


def build_proxy_rotator(settings: Settings) -> ProxyRotator:
    proxies = [p.strip() for p in settings.crawler_proxy_urls.split(",") if p.strip()]
    if not proxies:
        return NoOpProxyRotator()
    return RoundRobinProxyRotator(proxies)


def build_retry_strategy(settings: Settings) -> RetryStrategy:
    return ExponentialBackoffRetry(
        max_attempts=settings.retry_max_attempts,
        base_delay=settings.retry_base_delay_seconds,
    )
