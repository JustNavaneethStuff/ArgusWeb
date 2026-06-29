# Crawl Engine — Interview Deep Dive

## Why It Exists

The crawl engine is the throughput-critical front of the pipeline. It must fetch pages respectfully (robots.txt, rate limits), deduplicate work, and emit durable events for downstream extraction.

## Design Decisions

1. **Playwright over raw HTTP** — Handles dynamic/JS-rendered content; tradeoff is higher memory per browser context.
2. **Redis dedup + content hash** — Two layers: skip re-queueing (Redis) and skip re-processing unchanged content (SHA-256).
3. **Bounded in-flight consumption** — Semaphore limits concurrent fetches; offsets commit after handler completion.
4. **Per-job URL tracking** — `crawl_job_urls` table gives accurate job progress independent of global URL upserts.

## Tradeoffs

| Choice | Benefit | Cost |
|--------|---------|------|
| Playwright | Dynamic content | ~100MB+ per browser |
| At-least-once Kafka | No message loss | Requires idempotency |
| Domain rate limiting | Politeness | Lower peak throughput |
| Link cap (50/page) | Bounded fan-out | Incomplete site coverage |

## Scaling

- Scale crawler replicas horizontally (same consumer group)
- Redis rate limiter coordinates per-domain tokens globally
- Partition Kafka by domain/URL for ordering

## Failure Scenarios

- **Crash mid-fetch:** Offset not committed → message replayed → idempotency skip or re-fetch
- **Robots block:** Marked `skipped`, no retry
- **HTTP 4xx/5xx:** Published to retry → DLQ after max attempts

## Alternatives Considered

- **Scrapy:** Mature but less integrated with our Kafka/Playwright stack
- **Headless HTTP only:** Faster but misses SPA content
- **Central crawl queue DB:** Strong ordering but Kafka already provides durable buffering
