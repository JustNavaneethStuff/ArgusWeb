from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventBase(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    trace_id: str = ""
    job_id: UUID
    url: str
    normalized_url: str
    attempt: int = 1
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    model_config = {"ser_json_timestamps": "isoformat"}


class UrlDiscovered(EventBase):
    TOPIC: ClassVar[str] = "argus.url.discovered"
    depth: int = 0
    max_depth: int = 1
    allowed_domains: list[str] = Field(default_factory=list)


class HtmlFetched(EventBase):
    TOPIC: ClassVar[str] = "argus.html.fetched"
    storage_key: str
    checksum: str
    size_bytes: int
    http_status: int = 200
    fetch_duration_ms: float = 0.0
    content_type: str = "text/html"
    depth: int = 0
    max_depth: int = 1
    allowed_domains: list[str] = Field(default_factory=list)


class PageParsed(EventBase):
    TOPIC: ClassVar[str] = "argus.page.parsed"
    storage_key: str
    checksum: str
    title: str | None = None
    description: str | None = None
    language: str | None = None
    text_snippet: str | None = None
    extracted_links: list[str] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
    depth: int = 0


class UrlFailed(EventBase):
    TOPIC: ClassVar[str] = "argus.url.failed"
    error: str
    stage: str = "crawler"
