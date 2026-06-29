# Database ER Diagram

```mermaid
erDiagram
    crawl_jobs ||--o{ crawl_job_urls : tracks
    crawl_jobs ||--o{ urls : seeds
    urls ||--o| html_artifacts : stores
    urls ||--o| pages : indexes
    processed_events {
        uuid event_id PK
        string stage PK
        timestamp processed_at
    }
    crawl_job_urls {
        uuid id PK
        uuid job_id FK
        string normalized_url
        string status
        jsonb checkpoint
    }
    crawl_jobs {
        uuid id PK
        jsonb seed_urls
        int max_depth
        string status
    }
    urls {
        uuid id PK
        string normalized_url UK
        string domain
        string status
    }
    pages {
        uuid id PK
        uuid url_id FK
        string title
        text text_snippet
    }
```

## Key Indexes

- `urls.normalized_url` (unique)
- `urls.job_id`
- `crawl_job_urls (job_id, status)`
- `pages.title` / `pages.text_snippet` (GIN pg_trgm)

## Design Notes

- **Global URL identity** in `urls` — one row per normalized URL
- **Per-job progress** in `crawl_job_urls` — many jobs can reference the same URL independently
- **Idempotency ledger** in `processed_events` — `(event_id, stage)` unique
