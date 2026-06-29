from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession


class ProcessedEventRepositoryPort(Protocol):
    async def try_claim(self, session: AsyncSession, event_id: uuid.UUID, stage: str) -> bool:
        """Return True if this event+stage has not been processed yet (claim succeeded)."""
        ...


class CrawlJobUrlRepositoryPort(Protocol):
    async def ensure_queued(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        normalized_url: str,
        depth: int = 0,
    ) -> bool:
        """Insert queued row if not exists. Return True if newly queued."""
        ...

    async def transition(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        normalized_url: str,
        status: str,
        *,
        error: str | None = None,
        checkpoint: dict | None = None,
    ) -> None:
        ...

    async def count_by_status(self, session: AsyncSession, job_id: uuid.UUID) -> dict[str, int]:
        ...


class EventBusPort(Protocol):
    async def publish(self, topic: str, event: object) -> None:
        ...
