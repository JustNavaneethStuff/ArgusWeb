from __future__ import annotations

import asyncio
import time

from aiokafka import AIOKafkaProducer
from playwright.async_api import Browser, async_playwright

from argus_core.content_hash import ContentHashStore
from argus_core.database import create_session_factory
from argus_core.factories import build_proxy_rotator, build_rate_limiter, build_retry_strategy
from argus_core.redis_client import UrlDedupStore, create_redis
from argus_core.robots import RobotsChecker
from argus_core.settings import get_settings
from argus_core.storage import HtmlStorage, create_minio_client
from argus_core.url_utils import extract_domain, normalize_url, url_hash
from argus_events.kafka_client import (
    create_consumer,
    create_producer,
    deserialize_event,
    publish_event,
    publish_dlq,
)
from argus_events.schemas import HtmlFetched, UrlDiscovered, UrlFailed
from argus_events.topics import HTML_FETCHED, URL_DISCOVERED, URL_FAILED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import (
    crawl_duration_seconds,
    kafka_messages_total,
    rate_limit_waits_total,
    urls_crawled_total,
    urls_failed_total,
    urls_unchanged_total,
)
from argus_observability.tracing import configure_tracing, get_tracer
from argus_observability.worker_metrics import start_metrics_server

logger = get_logger(__name__)


class CrawlerWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.robots = RobotsChecker(self.settings)
        self.retry_strategy = build_retry_strategy(self.settings)
        self._browser: Browser | None = None
        self._semaphore = asyncio.Semaphore(self.settings.crawler_concurrency)
        self._tracer = get_tracer("argus.crawler")

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.redis = await create_redis(self.settings)
        self.dedup = UrlDedupStore(self.redis)
        self.rate_limiter = build_rate_limiter(self.settings, self.redis)
        self.proxy_rotator = build_proxy_rotator(self.settings)
        self.session_factory = create_session_factory(self.settings)
        self.content_hashes = ContentHashStore(self.session_factory, self.redis)
        self.storage = HtmlStorage(create_minio_client(self.settings), self.settings.minio_bucket)
        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-crawlers",
            [URL_DISCOVERED],
        )

        playwright = await async_playwright().start()
        self._browser = await playwright.chromium.launch(headless=True)
        logger.info("crawler_started")

        try:
            async for message in self.consumer:
                event = deserialize_event(message.value, UrlDiscovered)
                asyncio.create_task(self._handle_with_semaphore(event))
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            if self._browser:
                await self._browser.close()
            await self.redis.aclose()

    async def _handle_with_semaphore(self, event: UrlDiscovered) -> None:
        async with self._semaphore:
            await self._handle(event)

    async def _handle(self, event: UrlDiscovered) -> None:
        h = url_hash(event.normalized_url)
        if event.force_recrawl:
            await self.dedup.release(h)
        elif not await self.dedup.try_claim(h, ttl_seconds=self.settings.incremental_recrawl_ttl_seconds):
            logger.info("url_duplicate_skipped", url=event.normalized_url)
            kafka_messages_total.labels(topic=URL_DISCOVERED, status="duplicate").inc()
            return

        domain = extract_domain(event.normalized_url)
        before = time.perf_counter()
        await self.rate_limiter.acquire(domain)
        if time.perf_counter() - before > 0.01:
            rate_limit_waits_total.labels(domain=domain).inc()

        if not await self.robots.is_allowed(event.url):
            logger.info("robots_disallowed", url=event.normalized_url)
            urls_crawled_total.labels(status="robots_blocked").inc()
            kafka_messages_total.labels(topic=URL_DISCOVERED, status="robots_blocked").inc()
            return

        with self._tracer.start_as_current_span("crawl.fetch") as span:
            span.set_attribute("url", event.normalized_url)
            start = time.perf_counter()
            try:
                html, status = await self._fetch(event.url)
                duration_ms = (time.perf_counter() - start) * 1000
                crawl_duration_seconds.observe(duration_ms / 1000)

                storage_key, checksum, size = self.storage.upload(str(event.job_id), h, html)

                stored_hash = await self.content_hashes.get_hash(event.normalized_url)
                if stored_hash and stored_hash == checksum:
                    await self.content_hashes.touch_last_crawled(event.normalized_url)
                    urls_unchanged_total.inc()
                    urls_crawled_total.labels(status="unchanged").inc()
                    kafka_messages_total.labels(topic=URL_DISCOVERED, status="unchanged").inc()
                    logger.info("url_unchanged", url=event.normalized_url)
                    return

                fetched = HtmlFetched(
                    event_id=event.event_id,
                    trace_id=event.trace_id,
                    job_id=event.job_id,
                    url=event.url,
                    normalized_url=event.normalized_url,
                    attempt=event.attempt,
                    storage_key=storage_key,
                    checksum=checksum,
                    size_bytes=size,
                    http_status=status,
                    fetch_duration_ms=duration_ms,
                    depth=event.depth,
                    max_depth=event.max_depth,
                    allowed_domains=event.allowed_domains,
                )
                await publish_event(self.producer, HTML_FETCHED, fetched)
                urls_crawled_total.labels(status="success").inc()
                kafka_messages_total.labels(topic=HTML_FETCHED, status="success").inc()
                logger.info("url_fetched", url=event.normalized_url, status=status, duration_ms=duration_ms)

                await self._enqueue_discovered_links(event, html)

            except Exception as exc:
                urls_crawled_total.labels(status="error").inc()
                urls_failed_total.labels(stage="crawler").inc()
                kafka_messages_total.labels(topic=URL_DISCOVERED, status="error").inc()
                self.proxy_rotator.next_proxy()
                await self._publish_failure(self.producer, event, str(exc), stage="crawler")

    async def _publish_failure(
        self,
        producer: AIOKafkaProducer,
        event: UrlDiscovered,
        error: str,
        stage: str,
    ) -> None:
        decision = self.retry_strategy.decide(event.attempt, error)
        if decision.should_retry:
            failed = UrlFailed(
                job_id=event.job_id,
                url=event.url,
                normalized_url=event.normalized_url,
                attempt=event.attempt,
                error=error,
                stage=stage,
                depth=event.depth,
                max_depth=event.max_depth,
                allowed_domains=event.allowed_domains,
                force_recrawl=event.force_recrawl,
                trace_id=event.trace_id,
            )
            await publish_event(producer, URL_FAILED, failed)
        else:
            await publish_dlq(producer, event.model_dump(mode="json"), error)
        logger.error("crawl_failed", url=event.normalized_url, error=error)
    async def _fetch(self, url: str) -> tuple[bytes, int]:
        assert self._browser is not None
        proxy = self.proxy_rotator.next_proxy()
        context = await self._browser.new_context(
            user_agent=self.settings.crawler_user_agent,
            proxy={"server": proxy} if proxy else None,
        )
        page = await context.new_page()
        try:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=self.settings.crawler_timeout_seconds * 1000,
            )
            status = response.status if response else 0
            html = (await page.content()).encode("utf-8")
            return html, status
        finally:
            await context.close()

    async def _enqueue_discovered_links(self, event: UrlDiscovered, html: bytes) -> None:
        from argus_core.parser import extract_page_data, filter_links_by_domain

        if event.depth >= event.max_depth:
            return

        data = extract_page_data(html, event.url)
        links = filter_links_by_domain(data["extracted_links"], event.allowed_domains)

        for link in links[:50]:
            normalized = normalize_url(link)
            lh = url_hash(normalized)
            if not await self.dedup.mark_seen(str(event.job_id), lh):
                continue
            discovered = UrlDiscovered(
                job_id=event.job_id,
                url=link,
                normalized_url=normalized,
                depth=event.depth + 1,
                max_depth=event.max_depth,
                allowed_domains=event.allowed_domains,
                trace_id=event.trace_id,
            )
            await publish_event(self.producer, URL_DISCOVERED, discovered)


def main() -> None:
    worker = CrawlerWorker()
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
