# Kafka Pipeline — Interview Deep Dive

## Why It Exists

Decouples crawl stages so each can scale, fail, and deploy independently. Kafka provides durable buffering between bursty crawl output and slower parse/clean stages.

## Design Decisions

1. **Manual commit** — Commit after handler success for at-least-once delivery
2. **Idempotent effects** — `processed_events` table for exactly-once side effects
3. **DLQ + retry service** — Centralized backoff policy; workers publish failure facts
4. **Schema versioning** — `schema_version` field on all events for forward compatibility

## Tradeoffs

| Pattern | When to use | Argus choice |
|---------|-------------|--------------|
| At-most-once | Metrics, logs | Not for crawl work |
| At-least-once + idempotent writes | Most pipelines | **Yes** |
| Exactly-once Kafka transactions | Financial | Overkill here |

## Scaling

- Separate consumer groups per stage
- Increase partitions for parallelism
- Message keys by URL for per-URL ordering

## Failure Scenarios

- **Consumer rebalance during processing:** Uncommitted offset → replay
- **Duplicate delivery:** Idempotency table skips second processing
- **Retrying poison message:** DLQ after retries; manual replay via dlq-replay service

## Alternatives

- **RabbitMQ:** Simpler ops, less replay/log retention
- **SQS:** Managed but no ordering without FIFO
- **Direct HTTP chaining:** No buffering, tight coupling
