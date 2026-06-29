# ADR 003: Per-Job URL Accounting via crawl_job_urls

## Status

Accepted

## Context

`urls.normalized_url` is globally unique with `job_id` on the row. Progress queries counted `Url` rows by `job_id`, but cleaner upserts by normalized URL without re-associating existing URLs to new jobs. `urls_queued` reflected seeds only, not discovered links.

## Decision

Add `crawl_job_urls` junction table tracking per-job URL status: queued, claimed, fetched, parsed, skipped, failed. Job progress reads from this table. Global `urls` remains the canonical indexed page store.

## Consequences

**Pros:** Accurate job dashboards; same URL in multiple jobs tracked independently.

**Cons:** Extra writes at queue time; migration not needed (greenfield table).

## Scaling

Index on `(job_id, status)` for progress aggregation. Batch stats queries replace N+1 loops.
