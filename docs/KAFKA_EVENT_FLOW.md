# Kafka Event Flow

## Topics

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `argus.url.discovered` | Scheduler, Crawler, Retry | Crawler | `UrlDiscovered` |
| `argus.html.fetched` | Crawler | Parser | `HtmlFetched` |
| `argus.page.parsed` | Parser, Retry | Cleaner | `PageParsed` |
| `argus.url.failed` | Crawler, Parser, Cleaner | Retry | `UrlFailed` |
| `argus.dlq` | Retry, workers | dlq-replay | envelope |

## Sequence: Happy Path

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant K as Kafka
    participant C as Crawler
    participant M as MinIO
    participant P as Parser
    participant L as Cleaner
    participant D as PostgreSQL

    S->>K: UrlDiscovered
    K->>C: consume
    C->>M: upload HTML
    C->>K: HtmlFetched
    K->>P: consume
    P->>M: download
    P->>K: PageParsed
    K->>L: consume
    L->>D: upsert url/page
```

## Delivery Semantics

1. Consumer reads message with `enable_auto_commit=False`
2. Handler completes (including downstream publish)
3. Offset committed
4. `processed_events` prevents duplicate side effects on replay

## Message Versioning

All events include `schema_version: 1`. Breaking changes require a new version and dual consumers during migration.

## Partition Keys

Messages keyed by `normalized_url` for per-URL ordering within a partition.
