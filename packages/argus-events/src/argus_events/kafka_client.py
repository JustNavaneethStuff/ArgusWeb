from __future__ import annotations

import json
from typing import TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from argus_events.topics import DLQ

T = TypeVar("T", bound=BaseModel)


def serialize_event(event: BaseModel) -> bytes:
    return json.dumps(event.model_dump(mode="json")).encode("utf-8")


def deserialize_event(data: bytes, model: type[T]) -> T:
    return model.model_validate_json(data)


async def create_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v if isinstance(v, bytes) else serialize_event(v),
    )
    await producer.start()
    return producer


async def create_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
    )
    await consumer.start()
    return consumer


async def publish_event(producer: AIOKafkaProducer, topic: str, event: BaseModel) -> None:
    headers = []
    trace_id = getattr(event, "trace_id", None)
    if trace_id:
        headers.append(("trace_id", trace_id.encode("utf-8")))
    await producer.send_and_wait(topic, serialize_event(event), headers=headers)


async def publish_dlq(producer: AIOKafkaProducer, payload: dict, error: str) -> None:
    message = {"error": error, "payload": payload}
    await producer.send_and_wait(DLQ, json.dumps(message).encode("utf-8"))
