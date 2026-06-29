# Data Model — Interview Deep Dive

## Why It Exists

PostgreSQL is the source of truth for indexed pages, job metadata, and operational visibility. Schema separates global URL identity from per-job crawl attempts.

## Design Decisions

1. **Global unique `normalized_url`** — Single canonical page record; upsert on clean
2. **`crawl_job_urls` junction** — Per-job status without duplicating page content
3. **MinIO for raw HTML** — Cheap blob storage; Postgres holds metadata + search fields
4. **pg_trgm search** — Good enough for portfolio scale; upgrade path to OpenSearch

## Tradeoffs

| Model | Pros | Cons |
|-------|------|------|
| URL per job row | Simple queries | Duplicate pages across jobs |
| Global URL + job junction | Accurate progress | More writes |
| Event sourcing only | Full audit trail | Complex read paths |

## Scaling

- Index `(job_id, status)` for progress queries
- Partition `urls` by domain at very large scale (future)
- Connection pooling via `DatabaseManager`

## Failure Scenarios

- **Cleaner retry:** Replays `PageParsed` event without re-crawl
- **Concurrent upserts:** Unique constraint on `normalized_url` prevents duplicates
- **Stale job progress:** Fixed by reading `crawl_job_urls` not global `urls.job_id`

## Alternatives

- **MongoDB for pages:** Flexible schema but weaker search/joins
- **Elasticsearch primary store:** Better search, harder transactional guarantees
