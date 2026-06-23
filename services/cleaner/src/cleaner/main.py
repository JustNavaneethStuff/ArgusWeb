from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from argus_core.content_hash import ContentHashStore
from argus_core.database import create_session_factory
from argus_core.factories import build_retry_strategy
from argus_core.models import HtmlArtifact, Page, Url, UrlStatus
from argus_core.redis_client import create_redis
from argus_core.settings import get_settings
from argus_core.url_utils import extract_domain
from argus_events.kafka_client import (
    create_consumer,
    create_producer,
    deserialize_event,
    publish_dlq,
    publish_event,
)
from argus_events.schemas import PageParsed, UrlFailed
from argus_events.topics import PAGE_PARSED, URL_FAILED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import kafka_messages_total, urls_failed_total
from argus_observability.tracing import configure_tracing, get_tracer
from argus_observability.worker_metrics import start_metrics_server

logger = get_logger(__name__)


def _clean_text(value: str | None, max_len: int = 10000) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned[:max_len] if cleaned else None


def _dedupe_links(links: list[str]) -> list[str]:
    seen: set[str] = set()
    result = []
    for link in links:
        if link not in seen:
            seen.add(link)
            result.append(link)
    return result


class CleanerWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.retry_strategy = build_retry_strategy(self.settings)
        self._tracer = get_tracer("argus.cleaner")

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.session_factory = create_session_factory(self.settings)
        self.redis = await create_redis(self.settings)
        self.content_hashes = ContentHashStore(self.session_factory, self.redis)
        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-cleaners",
            [PAGE_PARSED],
        )

        logger.info("cleaner_started")
        try:
            async for message in self.consumer:
                event = deserialize_event(message.value, PageParsed)
                await self._handle(event)
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.redis.aclose()

    async def _handle(self, event: PageParsed) -> None:
        with self._tracer.start_as_current_span("cleaner.upsert") as span:
            span.set_attribute("url", event.normalized_url)
            try:
                async with self.session_factory() as session:
                    url_id = await self._upsert_url(session, event)
                    await self._upsert_artifact(session, url_id, event)
                    await self._upsert_page(session, url_id, event)
                    await session.commit()

                await self.content_hashes.set_hash(event.normalized_url, event.checksum)
                kafka_messages_total.labels(topic=PAGE_PARSED, status="success").inc()
                logger.info("page_cleaned", url=event.normalized_url)
            except Exception as exc:
                urls_failed_total.labels(stage="cleaner").inc()
                kafka_messages_total.labels(topic=PAGE_PARSED, status="error").inc()
                decision = self.retry_strategy.decide(event.attempt, str(exc))
                if decision.should_retry:
                    failed = UrlFailed(
                        job_id=event.job_id,
                        url=event.url,
                        normalized_url=event.normalized_url,
                        attempt=event.attempt,
                        error=str(exc),
                        stage="cleaner",
                        depth=event.depth,
                        storage_key=event.storage_key,
                        checksum=event.checksum,
                        page_payload=event.model_dump(mode="json"),
                        trace_id=event.trace_id,
                    )
                    await publish_event(self.producer, URL_FAILED, failed)
                else:
                    await publish_dlq(self.producer, event.model_dump(mode="json"), str(exc))
                logger.error("clean_failed", url=event.normalized_url, error=str(exc))

    async def _upsert_url(self, session, event: PageParsed) -> uuid.UUID:
        now = datetime.now(UTC)
        result = await session.execute(
            select(Url).where(Url.normalized_url == event.normalized_url)
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.status = UrlStatus.parsed
            existing.content_hash = event.checksum
            existing.last_crawled_at = now
            return existing.id

        url = Url(
            job_id=event.job_id,
            canonical_url=event.url,
            normalized_url=event.normalized_url,
            domain=extract_domain(event.normalized_url),
            status=UrlStatus.parsed,
            content_hash=event.checksum,
            depth=event.depth,
            last_crawled_at=now,
        )
        session.add(url)
        await session.flush()
        return url.id

    async def _upsert_artifact(self, session, url_id: uuid.UUID, event: PageParsed) -> None:
        result = await session.execute(select(HtmlArtifact).where(HtmlArtifact.url_id == url_id))
        existing = result.scalar_one_or_none()
        if existing:
            existing.storage_key = event.storage_key
            existing.checksum = event.checksum
            return

        session.add(
            HtmlArtifact(
                url_id=url_id,
                storage_key=event.storage_key,
                checksum=event.checksum,
                size_bytes=0,
            )
        )

    async def _upsert_page(self, session, url_id: uuid.UUID, event: PageParsed) -> None:
        links = _dedupe_links(event.extracted_links)
        title = _clean_text(event.title, 1024)
        description = _clean_text(event.description)
        snippet = _clean_text(event.text_snippet, 500)

        result = await session.execute(select(Page).where(Page.url_id == url_id))
        existing = result.scalar_one_or_none()
        if existing:
            existing.title = title
            existing.description = description
            existing.language = event.language
            existing.text_snippet = snippet
            existing.extracted_links = links
            existing.page_metadata = event.metadata
            return

        session.add(
            Page(
                url_id=url_id,
                title=title,
                description=description,
                language=event.language,
                text_snippet=snippet,
                extracted_links=links,
                page_metadata=event.metadata,
            )
        )


def main() -> None:
    worker = CleanerWorker()
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
