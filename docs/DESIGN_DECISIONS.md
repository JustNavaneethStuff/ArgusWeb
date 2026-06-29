# Design Decisions Summary

This document captures the audit findings and rationale for the Production Platform refactor. See [docs/adr/](adr/) for formal ADRs.

## Audit: Key Weaknesses (Before)

| Issue | Impact | Fix |
|-------|--------|-----|
| Kafka auto-commit | Message loss on crash | Manual commit after handler |
| Fire-and-forget crawler tasks | Unbounded backlog | Bounded semaphore + await |
| No idempotency | Duplicate DB writes on replay | `processed_events` table |
| Global URL as job progress | Inaccurate dashboards | `crawl_job_urls` junction |
| Monolithic argus-core | Hard to test/evolve | Clean Architecture layers |
| Shallow extraction | Limited data quality | Typed extraction engine |
| Engine per health check | Connection pool leak | `DatabaseManager` singleton |
| GET mutates job status | Side effects on read | `refresh_job_status` separate |

## Why Production Engineering Matters

Interviewers at top firms evaluate whether you understand **delivery guarantees**, **failure modes**, and **operational visibility** — not just whether you can fetch a webpage. Argus demonstrates:

1. **At-least-once Kafka** with **exactly-once effects**
2. **Separation of concerns** across pipeline stages
3. **Observable** distributed system (metrics, traces, health probes)
4. **Testable** architecture with unit + integration coverage
5. **Documented tradeoffs** for every major decision

## Target Layer Responsibilities

- **Domain** — Pure business rules (status enums, checkpoint value objects)
- **Application** — Use cases (idempotency, crawl progress)
- **Ports** — Interfaces for repos and event bus
- **Infrastructure** — SQLAlchemy repos, Kafka adapters, composition root
- **Entrypoints** — FastAPI apps and worker mains (wire only)

## Incremental Migration Strategy

We did **not** big-bang rewrite. Each milestone preserves backward-compatible imports and event schemas while improving reliability. New code uses Clean Architecture paths; legacy modules delegate to new implementations.
