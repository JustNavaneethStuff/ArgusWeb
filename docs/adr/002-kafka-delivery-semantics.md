# ADR 002: Kafka Manual Commit and Bounded Consumption

## Status

Accepted

## Context

Consumers used `enable_auto_commit=True` and the crawler used fire-and-forget `asyncio.create_task`. Offsets could commit before fetch completed, causing silent message loss on crash.

## Decision

- Default `enable_auto_commit=False` for all workers.
- Commit offset only after handler completes successfully (including failure events published to Kafka).
- Crawler uses semaphore-bound synchronous await per message (no unbounded task backlog).

## Consequences

**Pros:** At-least-once delivery; crash recovery replays in-flight messages.

**Cons:** Duplicate delivery possible; requires idempotent handlers (`processed_events` table).

## Tradeoffs

| Approach | Pros | Cons |
|----------|------|------|
| Auto-commit (old) | Simple | Message loss on crash |
| Manual commit | Durable | Duplicates on replay |
| Transactional outbox | Strongest consistency | Higher complexity |

We chose manual commit + idempotent effects as the practical middle ground for a portfolio platform.
