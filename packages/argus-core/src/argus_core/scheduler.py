"""Phase 3: APScheduler cron job scheduling."""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.models import ScheduledCrawlJob


JobCallback = Callable[[dict], Awaitable[None]]


class JobScheduler(ABC):
    @abstractmethod
    async def schedule_cron(self, schedule_id: uuid.UUID, cron: str, job_config: dict) -> str:
        """Schedule a recurring job. Returns schedule id."""

    @abstractmethod
    async def cancel(self, schedule_id: str) -> None:
        """Cancel a scheduled job."""

    @abstractmethod
    async def load_from_db(self) -> None:
        """Load enabled schedules from database."""


class NoOpJobScheduler(JobScheduler):
    async def schedule_cron(self, schedule_id: uuid.UUID, cron: str, job_config: dict) -> str:
        return "noop"

    async def cancel(self, schedule_id: str) -> None:
        return None

    async def load_from_db(self) -> None:
        return None


class APSchedulerJobScheduler(JobScheduler):
    """APScheduler-based cron scheduler with DB persistence."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        job_callback: JobCallback,
    ) -> None:
        self._session_factory = session_factory
        self._job_callback = job_callback
        self._scheduler = AsyncIOScheduler()
        self._running = False

    def start(self) -> None:
        if not self._running:
            self._scheduler.start()
            self._running = True

    def shutdown(self) -> None:
        if self._running:
            self._scheduler.shutdown(wait=False)
            self._running = False

    async def _run_job(self, schedule_id: str, job_config: dict) -> None:
        async with self._session_factory() as session:
            schedule = await session.get(ScheduledCrawlJob, uuid.UUID(schedule_id))
            if schedule:
                schedule.last_run_at = datetime.now(UTC)
                job = self._scheduler.get_job(schedule_id)
                if job and job.next_run_time:
                    schedule.next_run_at = job.next_run_time.replace(tzinfo=UTC)
                await session.commit()

        await self._job_callback(job_config)

    async def schedule_cron(self, schedule_id: uuid.UUID, cron: str, job_config: dict) -> str:
        sid = str(schedule_id)
        parts = cron.strip().split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 fields: minute hour day month day_of_week")

        trigger = CronTrigger(
            minute=parts[0],
            hour=parts[1],
            day=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )

        self._scheduler.add_job(
            self._run_job,
            trigger=trigger,
            id=sid,
            args=[sid, job_config],
            replace_existing=True,
        )

        async with self._session_factory() as session:
            schedule = await session.get(ScheduledCrawlJob, schedule_id)
            if schedule:
                job = self._scheduler.get_job(sid)
                if job and job.next_run_time:
                    schedule.next_run_at = job.next_run_time.replace(tzinfo=UTC)
                await session.commit()

        return sid

    async def cancel(self, schedule_id: str) -> None:
        try:
            self._scheduler.remove_job(schedule_id)
        except Exception:
            pass

    async def load_from_db(self) -> None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(ScheduledCrawlJob).where(ScheduledCrawlJob.enabled.is_(True))
            )
            schedules = result.scalars().all()

        for schedule in schedules:
            await self.schedule_cron(schedule.id, schedule.cron_expression, schedule.job_config)
