"""Tests for Kafka client helpers."""

from __future__ import annotations

import uuid

from argus_events.kafka_client import extract_trace_id, serialize_event
from argus_events.schemas import UrlDiscovered


def test_extract_trace_id_from_headers():
    headers = [("trace_id", b"abc-123")]
    assert extract_trace_id(headers) == "abc-123"


def test_extract_trace_id_missing():
    assert extract_trace_id(None) is None
    assert extract_trace_id([]) is None


def test_event_serialization_includes_schema_version():
    event = UrlDiscovered(
        job_id=uuid.uuid4(),
        url="https://example.com",
        normalized_url="https://example.com",
    )
    raw = serialize_event(event)
    assert b"schema_version" in raw
