from __future__ import annotations

import asyncio

from argus_core.database import DatabaseManager
from argus_core.extraction.engine import ExtractionEngine
from argus_core.factories import build_retry_strategy
from argus_core.infrastructure.composition import build_worker_dependencies
from argus_core.settings import get_settings
from argus_core.storage import HtmlStorage, create_minio_client
from argus_events.kafka_client import (
    consume_with_backpressure,
    create_consumer,
    create_producer,
    deserialize_event,
    extract_trace_id,
    publish_dlq,
    publish_event,
)
from argus_events.schemas import HtmlFetched, PageParsed, UrlFailed
from argus_events.topics import HTML_FETCHED, PAGE_PARSED, URL_FAILED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import kafka_messages_total, parser_errors_total, urls_failed_total
from argus_observability.tracing import (
    attach_trace_context,
    configure_tracing,
    detach_trace_context,
    get_tracer,
)
from argus_observability.worker_metrics import start_metrics_server

logger = get_logger(__name__)


class ParserWorker:
    def __init__(self) -> None:
        self.deps = build_worker_dependencies()
        self.settings = self.deps.settings
        self.retry_strategy = build_retry_strategy(self.settings)
        self._tracer = get_tracer("argus.parser")
        self._engine = ExtractionEngine()

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.db = DatabaseManager(self.settings)
        self.storage = HtmlStorage(create_minio_client(self.settings), self.settings.minio_bucket)
        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-parsers",
            [HTML_FETCHED],
            enable_auto_commit=False,
        )

        logger.info("parser_started")
        try:
            await consume_with_backpressure(
                self.consumer,
                self._handle_message,
                max_in_flight=self.settings.kafka_max_in_flight,
            )
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.db.dispose()

    async def _handle_message(self, message) -> None:
        event = deserialize_event(message.value, HtmlFetched)
        header_trace = extract_trace_id(message.headers)
        if header_trace and not event.trace_id:
            event.trace_id = header_trace

        token = attach_trace_context(event.trace_id or header_trace)
        try:
            async with self.db.session_factory() as session:
                if not await self.deps.idempotency.should_process(
                    session, event.event_id, "parser"
                ):
                    await session.commit()
                    kafka_messages_total.labels(topic=HTML_FETCHED, status="duplicate").inc()
                    return
                await session.commit()
            await self._handle(event)
        finally:
            detach_trace_context(token)

    async def _handle(self, event: HtmlFetched) -> None:
        with self._tracer.start_as_current_span("parser.extract") as span:
            span.set_attribute("url", event.normalized_url)
            try:
                html = self.storage.download(event.storage_key)
                page = self._engine.extract(html, event.url, event.content_type)
                legacy = page.to_legacy_dict()

                parsed = PageParsed(
                    event_id=event.event_id,
                    trace_id=event.trace_id,
                    job_id=event.job_id,
                    url=event.url,
                    normalized_url=event.normalized_url,
                    attempt=event.attempt,
                    storage_key=event.storage_key,
                    checksum=event.checksum,
                    title=legacy["title"],
                    description=legacy["description"],
                    language=legacy["language"],
                    text_snippet=legacy["text_snippet"],
                    extracted_links=legacy["extracted_links"],
                    metadata=legacy["metadata"],
                    depth=event.depth,
                )
                await publish_event(
                    self.producer,
                    PAGE_PARSED,
                    parsed,
                    key=event.normalized_url.encode("utf-8"),
                )
                kafka_messages_total.labels(topic=PAGE_PARSED, status="success").inc()
                logger.info("page_parsed", url=event.normalized_url, title=legacy["title"])
            except Exception as exc:
                parser_errors_total.labels(error_type=type(exc).__name__).inc()
                urls_failed_total.labels(stage="parser").inc()
                kafka_messages_total.labels(topic=HTML_FETCHED, status="error").inc()
                decision = self.retry_strategy.decide(event.attempt, str(exc))
                if decision.should_retry:
                    failed = UrlFailed(
                        job_id=event.job_id,
                        url=event.url,
                        normalized_url=event.normalized_url,
                        attempt=event.attempt,
                        error=str(exc),
                        stage="parser",
                        depth=event.depth,
                        max_depth=event.max_depth,
                        allowed_domains=event.allowed_domains,
                        trace_id=event.trace_id,
                    )
                    await publish_event(self.producer, URL_FAILED, failed)
                else:
                    await publish_dlq(self.producer, event.model_dump(mode="json"), str(exc))
                logger.error("parse_failed", url=event.normalized_url, error=str(exc))


def main() -> None:
    worker = ParserWorker()
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
