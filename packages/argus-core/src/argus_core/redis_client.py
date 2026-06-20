from __future__ import annotations

import redis.asyncio as aioredis

from argus_core.settings import Settings


async def create_redis(settings: Settings) -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=True)


async def check_redis(settings: Settings) -> bool:
    client = await create_redis(settings)
    try:
        return await client.ping()
    finally:
        await client.aclose()


class UrlDedupStore:
    """Redis-backed URL deduplication."""

    def __init__(self, redis: aioredis.Redis, prefix: str = "argus:url") -> None:
        self._redis = redis
        self._prefix = prefix

    def _key(self, url_hash: str) -> str:
        return f"{self._prefix}:{url_hash}"

    async def try_claim(self, url_hash: str, ttl_seconds: int = 86400) -> bool:
        """Return True if URL was not seen before (claimed successfully)."""
        return bool(await self._redis.set(self._key(url_hash), "1", nx=True, ex=ttl_seconds))

    async def mark_seen(self, job_id: str, url_hash: str) -> bool:
        """Track URL in job-specific set. Returns True if newly added."""
        return bool(await self._redis.sadd(f"argus:seen:{job_id}", url_hash))
