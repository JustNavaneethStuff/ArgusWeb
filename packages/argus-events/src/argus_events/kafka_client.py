from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from argus_events.topics import DLQ

T = TypeVar("T", bound=BaseModel)

Handler = Callable[[Any], Awaitable[None]]


def serialize_event(event: BaseModel) -> bytes:
    return json.dumps(event.model_dump(mode="json")).encode("utf-8")


def deserialize_event(data: bytes, model: type[T]) -> T:
    return model.model_validate_json(data)


def extract_trace_id(headers: list[tuple[str, bytes]] | None) -> str | None:
    if not headers:
        return None
    for key, value in headers:
        if key == "trace_id":
            return value.decode("utf-8")
    return None


async def create_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else serialize_event(v),
        acks="all",
        retries=3,
    )
    await producer.start()
    return producer


async def create_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    *,
    enable_auto_commit: bool = False,
) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda v: v,
    )
    await consumer.start()
    return consumer


async def publish_event(
    producer: AIOKafkaProducer,
    topic: str,
    event: BaseModel,
    *,
    key: bytes | None = None,
) -> None:
    headers = []
    trace_id = getattr(event, "trace_id", None)
    if trace_id:
        headers.append(("trace_id", trace_id.encode("utf-8")))
    await producer.send_and_wait(
        topic, serialize_event(event), headers=headers, key=key
    )


async def publish_dlq(producer: AIOKafkaProducer, payload: dict, error: str) -> None:
    message = {"error": error, "payload": payload}
    await producer.send_and_wait(DLQ, json.dumps(message).encode("utf-8"))


async def commit_message(consumer: AIOKafkaConsumer, message: Any) -> None:
    """Commit offset after successful processing (at-least-once delivery)."""
    await consumer.commit()


async def consume_with_backpressure(
    consumer: AIOKafkaConsumer,
    handler: Handler,
    *,
    max_in_flight: int = 10,
) -> None:
    """Bounded in-flight consumption with manual commit after each message."""
    semaphore = asyncio.Semaphore(max_in_flight)

    async for message in consumer:
        async with semaphore:
            await handler(message)
            await commit_message(consumer, message)
