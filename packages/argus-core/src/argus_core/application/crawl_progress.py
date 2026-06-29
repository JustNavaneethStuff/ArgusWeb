from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.domain.crawl import CrawlAttemptStatus
from argus_core.ports import CrawlJobUrlRepositoryPort


class CrawlProgressService:
    """Manages per-job URL status transitions and progress aggregation."""

    def __init__(self, repo: CrawlJobUrlRepositoryPort) -> None:
        self._repo = repo

    async def queue_url(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        normalized_url: str,
        depth: int = 0,
    ) -> bool:
        return await self._repo.ensure_queued(session, job_id, normalized_url, depth)

    async def mark(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        normalized_url: str,
        status: CrawlAttemptStatus,
        *,
        error: str | None = None,
        checkpoint: dict | None = None,
    ) -> None:
        await self._repo.transition(
            session,
            job_id,
            normalized_url,
            status.value,
            error=error,
            checkpoint=checkpoint,
        )

    async def status_counts(
        self, session: AsyncSession, job_id: uuid.UUID
    ) -> dict[str, int]:
        return await self._repo.count_by_status(session, job_id)
