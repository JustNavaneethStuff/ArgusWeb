from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.database import check_postgres, create_session_factory
from argus_core.jobs import compute_job_progress, list_jobs
from argus_core.models import CrawlJob, JobStatus, ScheduledCrawlJob, Url
from argus_core.redis_client import UrlDedupStore, create_redis
from argus_core.scheduler import APSchedulerJobScheduler
from argus_core.settings import get_settings
from argus_core.url_utils import extract_domain, normalize_url, url_hash
from argus_events.kafka_client import create_producer, publish_event
from argus_events.schemas import UrlDiscovered
from argus_events.topics import URL_DISCOVERED
from argus_observability.fastapi_metrics import metrics_endpoint
from argus_observability.logging import configure_logging, get_logger
from argus_observability.tracing import configure_tracing

logger = get_logger(__name__)
settings = get_settings()


class JobRequest(BaseModel):
    seed_urls: list[str] = Field(default_factory=list)
    max_depth: int = Field(default=1, ge=0, le=10)
    allowed_domains: list[str] | None = None
    incremental: bool = False
    recrawl_stale_hours: int = Field(default=24, ge=1, le=720)

    @model_validator(mode="after")
    def validate_urls(self) -> JobRequest:
        if not self.incremental and not self.seed_urls:
            raise ValueError("seed_urls required unless incremental=True")
        return self


class JobResponse(BaseModel):
    job_id: uuid.UUID
    urls_queued: int
    status: str


class ScheduleRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    cron_expression: str = Field(min_length=9, max_length=64)
    job_config: JobRequest


class SchedulePatch(BaseModel):
    enabled: bool | None = None
    cron_expression: str | None = None
    name: str | None = None


async def submit_job_internal(
    session_factory: async_sessionmaker[AsyncSession],
    dedup: UrlDedupStore,
    producer: AIOKafkaProducer,
    request: JobRequest,
) -> JobResponse:
    allowed_domains = request.allowed_domains or [
        extract_domain(url) for url in request.seed_urls
    ]
    if request.incremental and not allowed_domains:
        raise HTTPException(status_code=400, detail="allowed_domains required for incremental jobs")

    async with session_factory() as session:
        job = CrawlJob(
            seed_urls=request.seed_urls or allowed_domains,
            max_depth=request.max_depth,
            allowed_domains=allowed_domains,
            status=JobStatus.running,
            urls_queued=0,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

    queued = 0
    if request.incremental:
        queued = await _queue_incremental_urls(
            session_factory, dedup, producer, job, allowed_domains,
            request.recrawl_stale_hours, request.max_depth,
        )
    else:
        for seed in request.seed_urls:
            normalized = normalize_url(seed)
            h = url_hash(normalized)
            if not await dedup.mark_seen(str(job.id), h):
                continue
            event = UrlDiscovered(
                job_id=job.id,
                url=seed,
                normalized_url=normalized,
                depth=0,
                max_depth=request.max_depth,
                allowed_domains=allowed_domains,
            )
            await publish_event(producer, URL_DISCOVERED, event)
            queued += 1
            logger.info("url_queued", job_id=str(job.id), url=normalized)

    if queued == 0:
        raise HTTPException(status_code=400, detail="No URLs queued for crawl")

    async with session_factory() as session:
        db_job = await session.get(CrawlJob, job.id)
        if db_job:
            db_job.urls_queued = queued
            await session.commit()

    return JobResponse(job_id=job.id, urls_queued=queued, status=JobStatus.running.value)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(settings.otel_service_name)
    configure_tracing(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    app.state.settings = settings
    app.state.session_factory = create_session_factory(settings)
    app.state.redis = await create_redis(settings)
    app.state.dedup = UrlDedupStore(app.state.redis)
    app.state.producer = await create_producer(settings.kafka_bootstrap_servers)

    async def cron_callback(job_config: dict) -> None:
        req = JobRequest.model_validate(job_config)
        await submit_job_internal(
            app.state.session_factory,
            app.state.dedup,
            app.state.producer,
            req,
        )

    scheduler = APSchedulerJobScheduler(app.state.session_factory, cron_callback)
    scheduler.start()
    await scheduler.load_from_db()
    app.state.job_scheduler = scheduler

    logger.info("scheduler_started")
    yield

    scheduler.shutdown()
    await app.state.producer.stop()
    await app.state.redis.aclose()
    logger.info("scheduler_stopped")


app = FastAPI(title="Argus Scheduler", version="0.3.0", lifespan=lifespan)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness() -> dict:
    checks: dict[str, str] = {}
    try:
        await check_postgres(app.state.settings)
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
    try:
        if await app.state.redis.ping():
            checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
    try:
        await app.state.producer.client.fetch_all_metadata()
        checks["kafka"] = "ok"
    except Exception as exc:
        checks["kafka"] = f"error: {exc}"
    if any(v != "ok" for v in checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return {"status": "ready", "checks": checks}


async def _queue_incremental_urls(
    session_factory: async_sessionmaker[AsyncSession],
    dedup: UrlDedupStore,
    producer: AIOKafkaProducer,
    job: CrawlJob,
    allowed_domains: list[str],
    stale_hours: int,
    max_depth: int,
) -> int:
    cutoff = datetime.now(UTC) - timedelta(hours=stale_hours)
    queued = 0

    async with session_factory() as session:
        result = await session.execute(
            select(Url).where(
                Url.domain.in_(allowed_domains),
                or_(Url.last_crawled_at.is_(None), Url.last_crawled_at < cutoff),
            )
        )
        urls = result.scalars().all()

    for url_row in urls:
        h = url_hash(url_row.normalized_url)
        if not await dedup.mark_seen(str(job.id), h):
            continue
        event = UrlDiscovered(
            job_id=job.id,
            url=url_row.canonical_url,
            normalized_url=url_row.normalized_url,
            depth=url_row.depth,
            max_depth=max_depth,
            allowed_domains=allowed_domains,
            force_recrawl=True,
        )
        await publish_event(producer, URL_DISCOVERED, event)
        queued += 1
        logger.info("incremental_url_queued", job_id=str(job.id), url=url_row.normalized_url)

    return queued


@app.post("/jobs", response_model=JobResponse)
async def create_job(request: JobRequest) -> JobResponse:
    return await submit_job_internal(
        app.state.session_factory,
        app.state.dedup,
        app.state.producer,
        request,
    )


@app.get("/jobs")
async def get_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    async with app.state.session_factory() as session:
        items, total = await list_jobs(session, limit, offset)
    return {"total": total, "items": items}


@app.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID) -> dict:
    async with app.state.session_factory() as session:
        detail = await compute_job_progress(session, job_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Job not found")
    return detail


@app.post("/schedules")
async def create_schedule(request: ScheduleRequest) -> dict:
    schedule = ScheduledCrawlJob(
        name=request.name,
        cron_expression=request.cron_expression,
        job_config=request.job_config.model_dump(),
        enabled=True,
    )
    async with app.state.session_factory() as session:
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)

    scheduler: APSchedulerJobScheduler = app.state.job_scheduler
    await scheduler.schedule_cron(schedule.id, schedule.cron_expression, schedule.job_config)

    return {
        "id": str(schedule.id),
        "name": schedule.name,
        "cron_expression": schedule.cron_expression,
        "enabled": schedule.enabled,
        "job_config": schedule.job_config,
    }


@app.get("/schedules")
async def list_schedules() -> dict:
    async with app.state.session_factory() as session:
        result = await session.execute(
            select(ScheduledCrawlJob).order_by(ScheduledCrawlJob.created_at.desc())
        )
        schedules = result.scalars().all()
    return {
        "items": [
            {
                "id": str(s.id),
                "name": s.name,
                "cron_expression": s.cron_expression,
                "enabled": s.enabled,
                "job_config": s.job_config,
                "last_run_at": s.last_run_at.isoformat() if s.last_run_at else None,
                "next_run_at": s.next_run_at.isoformat() if s.next_run_at else None,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in schedules
        ]
    }


@app.get("/schedules/{schedule_id}")
async def get_schedule(schedule_id: uuid.UUID) -> dict:
    async with app.state.session_factory() as session:
        schedule = await session.get(ScheduledCrawlJob, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {
        "id": str(schedule.id),
        "name": schedule.name,
        "cron_expression": schedule.cron_expression,
        "enabled": schedule.enabled,
        "job_config": schedule.job_config,
        "last_run_at": schedule.last_run_at.isoformat() if schedule.last_run_at else None,
        "next_run_at": schedule.next_run_at.isoformat() if schedule.next_run_at else None,
    }


@app.patch("/schedules/{schedule_id}")
async def patch_schedule(schedule_id: uuid.UUID, patch: SchedulePatch) -> dict:
    scheduler: APSchedulerJobScheduler = app.state.job_scheduler
    async with app.state.session_factory() as session:
        schedule = await session.get(ScheduledCrawlJob, schedule_id)
        if not schedule:
            raise HTTPException(status_code=404, detail="Schedule not found")

        if patch.name is not None:
            schedule.name = patch.name
        if patch.enabled is not None:
            schedule.enabled = patch.enabled
        if patch.cron_expression is not None:
            schedule.cron_expression = patch.cron_expression
        await session.commit()
        await session.refresh(schedule)

    if not schedule.enabled:
        await scheduler.cancel(str(schedule_id))
    elif patch.cron_expression is not None or patch.enabled is True:
        await scheduler.schedule_cron(schedule.id, schedule.cron_expression, schedule.job_config)

    return {"id": str(schedule.id), "enabled": schedule.enabled}


@app.delete("/schedules/{schedule_id}")
async def delete_schedule(schedule_id: uuid.UUID) -> dict:
    scheduler: APSchedulerJobScheduler = app.state.job_scheduler
    await scheduler.cancel(str(schedule_id))
    async with app.state.session_factory() as session:
        schedule = await session.get(ScheduledCrawlJob, schedule_id)
        if not schedule:
            raise HTTPException(status_code=404, detail="Schedule not found")
        schedule.enabled = False
        await session.delete(schedule)
        await session.commit()
    return {"deleted": str(schedule_id)}


def main() -> None:
    import uvicorn

    uvicorn.run(
        "scheduler.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
