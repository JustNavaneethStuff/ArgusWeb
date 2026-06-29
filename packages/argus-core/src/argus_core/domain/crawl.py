from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class CrawlAttemptStatus(enum.StrEnum):
    """Per-job URL lifecycle status."""

    queued = "queued"
    claimed = "claimed"
    fetched = "fetched"
    parsed = "parsed"
    skipped = "skipped"
    failed = "failed"


@dataclass(frozen=True)
class CrawlCheckpoint:
    """Resume metadata for a crawl attempt."""

    stage: str
    storage_key: str | None = None
    checksum: str | None = None
    http_status: int | None = None
    updated_at: datetime | None = None
    extra: dict = field(default_factory=dict)
