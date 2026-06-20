from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.database import create_session_factory
from argus_core.models import CrawlJob, JobStatus
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
    seed_urls: list[str] = Field(min_length=1)
    max_depth: int = Field(default=1, ge=0, le=10)
    allowed_domains: list[str] | None = None


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


app = FastAPI(title="Argus Scheduler", version="0.1.0", lifespan=lifespan)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/jobs", response_model=JobResponse)
async def create_job(request: JobRequest) -> JobResponse:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    dedup: UrlDedupStore = app.state.dedup
    producer: AIOKafkaProducer = app.state.producer

    allowed_domains = request.allowed_domains or [
        extract_domain(url) for url in request.seed_urls
    ]

    async with session_factory() as session:
        job = CrawlJob(
            seed_urls=request.seed_urls,
            max_depth=request.max_depth,
            allowed_domains=allowed_domains,
            status=JobStatus.running,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

    queued = 0
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
        raise HTTPException(status_code=400, detail="All seed URLs were duplicates")

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
