from __future__ import annotations

import asyncio

from argus_core.parser import extract_page_data
from argus_core.settings import get_settings
from argus_core.storage import HtmlStorage, create_minio_client
from argus_events.kafka_client import (
    create_consumer,
    create_producer,
    deserialize_event,
    publish_event,
    publish_dlq,
)
from argus_events.schemas import HtmlFetched, PageParsed
from argus_events.topics import HTML_FETCHED, PAGE_PARSED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import kafka_messages_total, parser_errors_total
from argus_observability.worker_metrics import start_metrics_server
from argus_observability.tracing import configure_tracing, get_tracer

logger = get_logger(__name__)


class ParserWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._tracer = get_tracer("argus.parser")

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.storage = HtmlStorage(create_minio_client(self.settings), self.settings.minio_bucket)
        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-parsers",
            [HTML_FETCHED],
        )

        logger.info("parser_started")
        try:
            async for message in self.consumer:
                event = deserialize_event(message.value, HtmlFetched)
                await self._handle(event)
        finally:
            await self.consumer.stop()
            await self.producer.stop()

    async def _handle(self, event: HtmlFetched) -> None:
        with self._tracer.start_as_current_span("parser.extract") as span:
            span.set_attribute("url", event.normalized_url)
            try:
                html = self.storage.download(event.storage_key)
                data = extract_page_data(html, event.url)

                parsed = PageParsed(
                    event_id=event.event_id,
                    trace_id=event.trace_id,
                    job_id=event.job_id,
                    url=event.url,
                    normalized_url=event.normalized_url,
                    attempt=event.attempt,
                    storage_key=event.storage_key,
                    checksum=event.checksum,
                    title=data["title"],
                    description=data["description"],
                    language=data["language"],
                    text_snippet=data["text_snippet"],
                    extracted_links=data["extracted_links"],
                    metadata=data["metadata"],
                    depth=event.depth,
                )
                await publish_event(self.producer, PAGE_PARSED, parsed)
                kafka_messages_total.labels(topic=PAGE_PARSED, status="success").inc()
                logger.info("page_parsed", url=event.normalized_url, title=data["title"])
            except Exception as exc:
                parser_errors_total.labels(error_type=type(exc).__name__).inc()
                kafka_messages_total.labels(topic=HTML_FETCHED, status="error").inc()
                await publish_dlq(self.producer, event.model_dump(mode="json"), str(exc))
                logger.error("parse_failed", url=event.normalized_url, error=str(exc))


def main() -> None:
    worker = ParserWorker()
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
