from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.database import create_session_factory
from argus_core.models import CrawlJob, JobStatus, Url
from argus_core.redis_client import UrlDedupStore, create_redis
from argus_core.scheduler import NoOpJobScheduler
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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(settings.otel_service_name)
    configure_tracing(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    app.state.settings = settings
    app.state.session_factory = create_session_factory(settings)
    app.state.redis = await create_redis(settings)
    app.state.dedup = UrlDedupStore(app.state.redis)
    app.state.producer = await create_producer(settings.kafka_bootstrap_servers)
    app.state.job_scheduler = NoOpJobScheduler()

    logger.info("scheduler_started")
    yield

    await app.state.producer.stop()
    await app.state.redis.aclose()
    logger.info("scheduler_stopped")


app = FastAPI(title="Argus Scheduler", version="0.2.0", lifespan=lifespan)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


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
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    dedup: UrlDedupStore = app.state.dedup
    producer: AIOKafkaProducer = app.state.producer

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
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

    queued = 0
    if request.incremental:
        queued = await _queue_incremental_urls(
            session_factory,
            dedup,
            producer,
            job,
            allowed_domains,
            request.recrawl_stale_hours,
            request.max_depth,
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

    return JobResponse(job_id=job.id, urls_queued=queued, status=JobStatus.running.value)


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
