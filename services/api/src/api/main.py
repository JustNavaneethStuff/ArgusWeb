from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.database import DatabaseManager, check_postgres
from argus_core.jobs import compute_job_progress, get_expanded_stats, list_jobs
from argus_core.models import ScheduledCrawlJob
from argus_core.redis_client import create_redis
from argus_core.search import get_page_by_url_id, search_pages
from argus_core.settings import get_settings
from argus_events.kafka_client import create_producer
from argus_observability.fastapi_metrics import metrics_endpoint
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import search_duration_seconds, search_queries_total
from argus_observability.tracing import configure_tracing
from sqlalchemy import select

logger = get_logger(__name__)
settings = get_settings()


class SearchResultItem(BaseModel):
    title: str | None
    url: str
    normalized_url: str
    description: str | None
    text_snippet: str | None
    domain: str
    score: float


class SearchResponse(BaseModel):
    query: str
    total: int
    limit: int
    offset: int
    results: list[SearchResultItem]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(settings.otel_service_name)
    configure_tracing(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    app.state.settings = settings
    app.state.db = DatabaseManager(settings)
    app.state.session_factory = app.state.db.session_factory
    app.state.redis = await create_redis(settings)
    app.state.producer = await create_producer(settings.kafka_bootstrap_servers)

    logger.info("api_started")
    yield

    await app.state.producer.stop()
    await app.state.redis.aclose()
    await app.state.db.dispose()
    logger.info("api_stopped")


app = FastAPI(title="Argus Analytics API", version="0.3.0", lifespan=lifespan)
app.add_route("/metrics", metrics_endpoint)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness() -> dict:
    checks: dict[str, str] = {}
    try:
        await check_postgres(app.state.settings, app.state.db)
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"

    try:
        if await app.state.redis.ping():
            checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    try:
        producer: AIOKafkaProducer = app.state.producer
        await producer.client.fetch_all_metadata()
        checks["kafka"] = "ok"
    except Exception as exc:
        checks["kafka"] = f"error: {exc}"

    if any(v != "ok" for v in checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return {"status": "ready", "checks": checks}


@app.get("/stats")
async def stats() -> dict:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    async with session_factory() as session:
        return await get_expanded_stats(session)


@app.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1),
    offset: int = Query(default=0, ge=0),
) -> SearchResponse:
    limit = min(limit, settings.search_max_limit)
    start = time.perf_counter()
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory

    async with session_factory() as session:
        results, total = await search_pages(
            session,
            q,
            limit=limit,
            offset=offset,
            similarity_threshold=settings.search_similarity_threshold,
        )

    search_duration_seconds.observe(time.perf_counter() - start)
    search_queries_total.labels(status="success").inc()

    return SearchResponse(
        query=q,
        total=total,
        limit=limit,
        offset=offset,
        results=[
            SearchResultItem(
                title=r.title,
                url=r.url,
                normalized_url=r.normalized_url,
                description=r.description,
                text_snippet=r.text_snippet,
                domain=r.domain,
                score=r.score,
            )
            for r in results
        ],
    )


@app.get("/pages/{url_id}")
async def get_page(url_id: uuid.UUID) -> dict:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    async with session_factory() as session:
        page = await get_page_by_url_id(session, url_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    return page


@app.get("/jobs")
async def get_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    async with session_factory() as session:
        items, total = await list_jobs(session, limit, offset)
    return {"total": total, "items": items}


@app.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID) -> dict:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    async with session_factory() as session:
        detail = await compute_job_progress(session, job_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Job not found")
    return detail


@app.get("/schedules")
async def list_schedules() -> dict:
    session_factory: async_sessionmaker[AsyncSession] = app.state.session_factory
    async with session_factory() as session:
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


def main() -> None:
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
