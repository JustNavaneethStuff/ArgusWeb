from __future__ import annotations

import asyncio
import json

from argus_core.redis_client import create_redis
from argus_core.settings import get_settings
from argus_events.kafka_client import (
    consume_with_backpressure,
    create_consumer,
    create_producer,
    publish_event,
)
from argus_events.schemas import PageParsed, UrlDiscovered
from argus_events.topics import DLQ, PAGE_PARSED, URL_DISCOVERED
from argus_observability.logging import configure_logging, get_logger
from argus_observability.metrics import dlq_replays_total
from argus_observability.tracing import configure_tracing
from argus_observability.worker_metrics import start_metrics_server

logger = get_logger(__name__)


class DlqReplayConsumer:
    """Consumes argus.dlq and replays failed events with a capped retry count."""

    def __init__(self) -> None:
        self.settings = get_settings()

    async def start(self) -> None:
        configure_logging(self.settings.otel_service_name)
        configure_tracing(self.settings.otel_service_name, self.settings.otel_exporter_otlp_endpoint)
        start_metrics_server(9091)

        self.redis = await create_redis(self.settings)
        self.producer = await create_producer(self.settings.kafka_bootstrap_servers)
        self.consumer = await create_consumer(
            self.settings.kafka_bootstrap_servers,
            "argus-dlq-replay",
            [DLQ],
            enable_auto_commit=False,
        )

        logger.info("dlq_replay_started")
        try:
            await consume_with_backpressure(
                self.consumer,
                lambda msg: self._handle(msg.value),
                max_in_flight=self.settings.kafka_max_in_flight,
            )
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.redis.aclose()

    async def _handle(self, raw: bytes) -> None:
        envelope = json.loads(raw.decode("utf-8"))
        payload = envelope.get("payload", {})
        error = envelope.get("error", "unknown")
        event_id = str(payload.get("event_id", "unknown"))

        replay_key = f"argus:dlq:replays:{event_id}"
        count = await self.redis.incr(replay_key)
        await self.redis.expire(replay_key, 86400)

        if count > self.settings.dlq_max_replays:
            dlq_replays_total.labels(result="cap_exceeded").inc()
            logger.warning("dlq_replay_cap_exceeded", event_id=event_id, error=error)
            return

        try:
            if "storage_key" in payload and "checksum" in payload and "extracted_links" in payload:
                event = PageParsed.model_validate(payload)
                await publish_event(self.producer, PAGE_PARSED, event)
            elif "normalized_url" in payload and "job_id" in payload:
                event = UrlDiscovered.model_validate(payload)
                event.force_recrawl = True
                await publish_event(self.producer, URL_DISCOVERED, event)
            else:
                dlq_replays_total.labels(result="unrecognized").inc()
                logger.error("dlq_unrecognized_payload", event_id=event_id)
                return

            dlq_replays_total.labels(result="success").inc()
            logger.info("dlq_replayed", event_id=event_id)
        except Exception as exc:
            dlq_replays_total.labels(result="error").inc()
            logger.error("dlq_replay_failed", event_id=event_id, error=str(exc))


def main() -> None:
    consumer = DlqReplayConsumer()
    asyncio.run(consumer.start())


if __name__ == "__main__":
    main()
