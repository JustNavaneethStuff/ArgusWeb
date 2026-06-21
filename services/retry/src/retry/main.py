from __future__ import annotations

import asyncio

from argus_core.factories import build_retry_strategy
from argus_core.settings import get_settings
from argus_events.kafka_client import (
    create_consumer,
    create_producer,
    deserialize_event,
    publish_event,
    publish_dlq,
)
from argus_events.schemas import PageParsed, UrlDiscovered, UrlFailed
from argus_events.topics import PAGE_PARSED, URL_DISCOVERED, URL_FAILED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import retry_attempts_total
from argus_observability.tracing import configure_tracing, get_tracer
from argus_observability.worker_metrics import start_metrics_server

logger = get_logger(__name__)


class RetryWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.retry_strategy = build_retry_strategy(self.settings)
        self._tracer = get_tracer("argus.retry")

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-retry",
            [URL_FAILED],
        )

        logger.info("retry_worker_started")
        try:
            async for message in self.consumer:
                event = deserialize_event(message.value, UrlFailed)
                await self._handle(event)
        finally:
            await self.consumer.stop()
            await self.producer.stop()

    async def _handle(self, event: UrlFailed) -> None:
        with self._tracer.start_as_current_span("retry.handle") as span:
            span.set_attribute("url", event.normalized_url)
            span.set_attribute("stage", event.stage)

            decision = self.retry_strategy.decide(event.attempt, event.error)
            if not decision.should_retry:
                await publish_dlq(self.producer, event.model_dump(mode="json"), event.error)
                retry_attempts_total.labels(stage=event.stage, result="dlq").inc()
                logger.warning("retry_exhausted", url=event.normalized_url, stage=event.stage)
                return

            await asyncio.sleep(decision.delay_seconds)

            if event.stage == "cleaner" and event.page_payload:
                parsed = PageParsed.model_validate(event.page_payload)
                parsed.attempt = event.attempt + 1
                await publish_event(self.producer, PAGE_PARSED, parsed)
            else:
                discovered = UrlDiscovered(
                    job_id=event.job_id,
                    url=event.url,
                    normalized_url=event.normalized_url,
                    attempt=event.attempt + 1,
                    depth=event.depth,
                    max_depth=event.max_depth,
                    allowed_domains=event.allowed_domains,
                    force_recrawl=event.force_recrawl,
                    trace_id=event.trace_id,
                )
                await publish_event(self.producer, URL_DISCOVERED, discovered)

            retry_attempts_total.labels(stage=event.stage, result="republished").inc()
            logger.info(
                "retry_republished",
                url=event.normalized_url,
                stage=event.stage,
                attempt=event.attempt + 1,
                delay=decision.delay_seconds,
            )


def main() -> None:
    worker = RetryWorker()
    asyncio.run(worker.start())


if __name__ == "__main__":
    main()
