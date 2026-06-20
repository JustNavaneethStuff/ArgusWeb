from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus_core.database import check_postgres, create_session_factory
from argus_core.models import CrawlJob, Page, Url, UrlStatus
from argus_core.redis_client import create_redis
from argus_core.settings import get_settings
from argus_events.kafka_client import create_producer
from argus_observability.fastapi_metrics import metrics_endpoint
from argus_observability.logging import configure_logging, get_logger
from argus_observability.tracing import configure_tracing

logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(settings.otel_service_name)
    configure_tracing(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    app.state.settings = settings
    app.state.session_factory = create_session_factory(settings)
    app.state.redis = await create_redis(settings)
    app.state.producer = await create_producer(settings.kafka_bootstrap_servers)

    logger.info("api_started")
    yield

    await app.state.producer.stop()
    await app.state.redis.aclose()
    logger.info("api_stopped")


app = FastAPI(title="Argus Analytics API", version="0.1.0", lifespan=lifespan)
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
        jobs = await session.scalar(select(func.count()).select_from(CrawlJob))
        pages = await session.scalar(select(func.count()).select_from(Page))
        urls_by_status = {}
        for status in UrlStatus:
            count = await session.scalar(
                select(func.count()).select_from(Url).where(Url.status == status)
            )
            urls_by_status[status.value] = count

    return {
        "jobs_total": jobs,
        "pages_indexed": pages,
        "urls_by_status": urls_by_status,
    }


@app.get("/search")
async def search(q: str = Query(..., min_length=1)) -> dict:
    """Phase 3 scaffold: full-text search not yet implemented."""
    raise HTTPException(
        status_code=501,
        detail={
            "message": "Search API is planned for Phase 3 (pg_trgm full-text search)",
            "query": q,
        },
    )


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
