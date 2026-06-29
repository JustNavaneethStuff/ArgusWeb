# Argus Architecture

## Overview

Argus is an event-driven distributed data extraction platform. Work flows through Kafka topics from job submission to indexed storage, with Redis for deduplication and rate limiting, MinIO for raw HTML, and PostgreSQL for structured page data.

```
Scheduler → url.discovered → Crawler → html.fetched → Parser → page.parsed → Cleaner → PostgreSQL
                                      ↘ url.failed → Retry / DLQ
```

## Clean Architecture Layers

| Layer | Location | Responsibility |
|-------|----------|----------------|
| **Domain** | `argus_core/domain/` | Entities, enums, policies (no I/O) |
| **Application** | `argus_core/application/` | Use cases orchestrating domain + ports |
| **Ports** | `argus_core/ports/` | Protocol interfaces (repositories, event bus) |
| **Infrastructure** | `argus_core/infrastructure/` | SQLAlchemy, Redis, MinIO adapters |
| **Entrypoints** | `services/*/main.py` | Wire dependencies, HTTP/Kafka boundaries |

Dependency rule: outer layers depend on inner layers; domain has zero infrastructure imports.

## Delivery Guarantees

| Component | Guarantee | Mechanism |
|-----------|-----------|-----------|
| Kafka consumption | At-least-once | Manual offset commit after handler success |
| Side effects | Exactly-once effects | `processed_events (event_id, stage)` dedup table |
| URL dedup | Best-effort skip | Redis TTL keys per normalized URL |
| DB upserts | Idempotent | Unique constraints + upsert by normalized URL |

## Per-Job URL Accounting

Canonical URLs live in `urls`. Per-job progress is tracked in `crawl_job_urls` with explicit status transitions: `queued → claimed → fetched → parsed | skipped | failed`.

This separates global URL identity from job-specific crawl attempts and fixes progress reporting when the same URL appears in multiple jobs.

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| Crawler crash mid-fetch | Offset not committed; message replayed |
| Duplicate Kafka message | `processed_events` skip; no double side effect |
| Parser failure | `UrlFailed` → retry service with backoff → DLQ |
| Cleaner failure with payload | Retry replays `PageParsed` without re-crawl |
| Scheduler replica conflict | APScheduler single-leader recommended for prod |

## Scaling

- **Crawler**: Horizontal scale via consumer group `argus-crawlers`; domain rate limits in Redis
- **Parser/Cleaner**: Scale consumer group replicas; idempotency prevents duplicate writes
- **API**: Stateless; connection pool via `DatabaseManager`
- **Kafka**: Partition by domain or normalized URL key for ordering per URL

See `docs/adr/` for design decision records and `docs/interview/` for subsystem deep-dives.
