from __future__ import annotations

from datetime import UTC, datetime

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.models import Url
from argus_core.url_utils import url_hash


class ContentHashStore:
    """Lookup and cache content hashes for incremental crawl."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis: aioredis.Redis | None = None,
        cache_ttl: int = 3600,
    ) -> None:
        self._session_factory = session_factory
        self._redis = redis
        self._cache_ttl = cache_ttl

    def _cache_key(self, normalized_url: str) -> str:
        return f"argus:hash:{url_hash(normalized_url)}"

    async def get_hash(self, normalized_url: str) -> str | None:
        if self._redis:
            cached = await self._redis.get(self._cache_key(normalized_url))
            if cached:
                return cached

        async with self._session_factory() as session:
            result = await session.execute(
                select(Url.content_hash).where(Url.normalized_url == normalized_url)
            )
            row = result.scalar_one_or_none()
            if row and self._redis:
                await self._redis.set(self._cache_key(normalized_url), row, ex=self._cache_ttl)
            return row

    async def set_hash(self, normalized_url: str, content_hash: str) -> None:
        if self._redis:
            await self._redis.set(self._cache_key(normalized_url), content_hash, ex=self._cache_ttl)

    async def touch_last_crawled(self, normalized_url: str) -> None:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            await session.execute(
                update(Url)
                .where(Url.normalized_url == normalized_url)
                .values(last_crawled_at=now)
            )
            await session.commit()
