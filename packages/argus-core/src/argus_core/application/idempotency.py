from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.ports import ProcessedEventRepositoryPort


class IdempotencyService:
    """Ensures exactly-once side effects per (event_id, stage)."""

    def __init__(self, repo: ProcessedEventRepositoryPort) -> None:
        self._repo = repo

    async def should_process(
        self, session: AsyncSession, event_id: uuid.UUID, stage: str
    ) -> bool:
        return await self._repo.try_claim(session, event_id, stage)
