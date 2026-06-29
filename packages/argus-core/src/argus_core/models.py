from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class UrlStatus(str, enum.Enum):
    pending = "pending"
    fetched = "fetched"
    parsed = "parsed"
    failed = "failed"


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    seed_urls: Mapped[list] = mapped_column(JSONB, nullable=False)
    max_depth: Mapped[int] = mapped_column(Integer, default=1)
    allowed_domains: Mapped[list] = mapped_column(JSONB, nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.pending)
    urls_queued: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    urls: Mapped[list["Url"]] = relationship(back_populates="job")


class Url(Base):
    __tablename__ = "urls"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("crawl_jobs.id"))
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_url: Mapped[str] = mapped_column(String(2048), unique=True, nullable=False, index=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[UrlStatus] = mapped_column(Enum(UrlStatus), default=UrlStatus.pending)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    last_crawled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    job: Mapped["CrawlJob"] = relationship(back_populates="urls")
    html_artifact: Mapped["HtmlArtifact | None"] = relationship(back_populates="url")
    page: Mapped["Page | None"] = relationship(back_populates="url")


class HtmlArtifact(Base):
    __tablename__ = "html_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("urls.id"), unique=True)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    http_status: Mapped[int] = mapped_column(Integer, default=200)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    url: Mapped["Url"] = relationship(back_populates="html_artifact")


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("urls.id"), unique=True)
    title: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    language: Mapped[str | None] = mapped_column(String(16), nullable=True)
    text_snippet: Mapped[str | None] = mapped_column(Text, nullable=True)
    extracted_links: Mapped[list] = mapped_column(JSONB, default=list)
    page_metadata: Mapped[dict] = mapped_column("metadata", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    url: Mapped["Url"] = relationship(back_populates="page")


class ScheduledCrawlJob(Base):
    __tablename__ = "scheduled_crawl_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(64), nullable=False)
    job_config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    enabled: Mapped[bool] = mapped_column(default=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ProcessedEvent(Base):
    """Idempotency ledger: one row per (event_id, pipeline stage)."""

    __tablename__ = "processed_events"
    __table_args__ = (UniqueConstraint("event_id", "stage", name="uq_processed_events_event_stage"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    stage: Mapped[str] = mapped_column(String(64), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CrawlJobUrl(Base):
    """Per-job URL attempt tracking for accurate progress and checkpoints."""

    __tablename__ = "crawl_job_urls"
    __table_args__ = (
        UniqueConstraint("job_id", "normalized_url", name="uq_crawl_job_urls_job_url"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("crawl_jobs.id"), index=True
    )
    normalized_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    checkpoint: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
