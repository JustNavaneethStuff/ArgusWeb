from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from argus_core.models import CrawlJobUrl, ProcessedEvent


class SqlAlchemyProcessedEventRepository:
    async def try_claim(
        self, session: AsyncSession, event_id: uuid.UUID, stage: str
    ) -> bool:
        stmt = (
            insert(ProcessedEvent)
            .values(
                event_id=event_id,
                stage=stage,
                processed_at=datetime.now(UTC),
            )
            .on_conflict_do_nothing(index_elements=["event_id", "stage"])
            .returning(ProcessedEvent.event_id)
        )
        result = await session.execute(stmt)
        row = result.first()
        return row is not None


class SqlAlchemyCrawlJobUrlRepository:
    async def ensure_queued(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        normalized_url: str,
        depth: int = 0,
    ) -> bool:
        stmt = (
            insert(CrawlJobUrl)
            .values(
                job_id=job_id,
                normalized_url=normalized_url,
                depth=depth,
                status="queued",
            )
            .on_conflict_do_nothing(index_elements=["job_id", "normalized_url"])
            .returning(CrawlJobUrl.id)
        )
        result = await session.execute(stmt)
        return result.first() is not None

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
        result = await session.execute(
            select(CrawlJobUrl).where(
                CrawlJobUrl.job_id == job_id,
                CrawlJobUrl.normalized_url == normalized_url,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = CrawlJobUrl(
                job_id=job_id,
                normalized_url=normalized_url,
                depth=0,
                status=status,
            )
            session.add(row)
        else:
            row.status = status
        if error is not None:
            row.error = error
        if checkpoint is not None:
            row.checkpoint = checkpoint
        row.updated_at = datetime.now(UTC)

    async def count_by_status(
        self, session: AsyncSession, job_id: uuid.UUID
    ) -> dict[str, int]:
        result = await session.execute(
            select(CrawlJobUrl.status, func.count())
            .where(CrawlJobUrl.job_id == job_id)
            .group_by(CrawlJobUrl.status)
        )
        return {status: int(count) for status, count in result.all()}
