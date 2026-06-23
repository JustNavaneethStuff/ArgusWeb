# Argus

Production-grade distributed web crawler and data extraction platform.

## Architecture

```
Scheduler → Kafka (url.discovered) → Crawler → MinIO + Kafka (html.fetched)
    → Parser → Kafka (page.parsed) → Cleaner → PostgreSQL → Analytics API → Dashboard
```

## Tech Stack

- **Python 3.12**, FastAPI, Playwright, BeautifulSoup
- **PostgreSQL**, Redis, Apache Kafka, MinIO
- **OpenTelemetry**, Prometheus, Grafana
- **React/Vite** dashboard

## Quick Start

```bash
cp .env.example .env
make up          # start all services
make migrate     # run database migrations (also runs on first boot)
```

### Submit a crawl job

```bash
curl -X POST http://localhost:8001/jobs \
  -H "Content-Type: application/json" \
  -d '{"seed_urls": ["https://example.com"], "max_depth": 1}'
```

### Endpoints

| Service    | URL                          |
|------------|------------------------------|
| API        | http://localhost:8000/docs   |
| Scheduler  | http://localhost:8001/docs   |
| Grafana    | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090        |
| MinIO      | http://localhost:9001        |
| Dashboard  | http://localhost:5173        |

## Development

```bash
# Install dependencies locally
make install

# Run unit tests
make test-unit

# Run integration tests (requires Docker)
make test-integration
```

## Project Structure

```
packages/argus-core          Shared models, URL utils, robots.txt
packages/argus-events        Kafka event schemas
packages/argus-observability Logging, tracing, metrics
services/scheduler           Job submission + scheduling
services/crawler             Playwright fetch workers
services/parser              HTML extraction
services/cleaner             Data normalization + DB upsert
services/retry               Failed URL retry with backoff
services/dlq_replay          DLQ message recovery
services/api                 Analytics + health API
frontend/dashboard           React product UI
```

## Phases

- **Phase 1**: Async crawl pipeline, robots.txt, dedup, HTML storage, metadata extraction
- **Phase 2**: Rate limiting, proxies, retry/DLQ replay, incremental crawl
- **Phase 3** (current): Search API, cron scheduling, full dashboard, alerting

## Phase 3 Features

### Search API

Full-text search over indexed pages using PostgreSQL pg_trgm:

```bash
curl "http://localhost:8000/search?q=example&limit=10"
```

### Job management

```bash
# List jobs
curl http://localhost:8000/jobs

# Job detail with progress
curl http://localhost:8000/jobs/{job_id}
```

### Cron scheduling

```bash
curl -X POST http://localhost:8001/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-example",
    "cron_expression": "0 0 * * *",
    "job_config": {"seed_urls": ["https://example.com"], "max_depth": 1}
  }'

# List schedules
curl http://localhost:8000/schedules
```

### Dashboard

The React dashboard at http://localhost:5173 provides:
- Search with ranked results
- Job submission and listing
- Cron schedule management
- Enhanced statistics with domain breakdown

### Alerting

Grafana unified alerting is provisioned with rules for:
- High crawl error rate
- Parser error spikes
- DLQ backlog
- Service down

View alerts in Grafana at http://localhost:3000/alerting

## Phase 2 Features

### Rate limiting

Per-domain Redis token bucket (default 1 req/s, burst 5). Configure via `RATE_LIMIT_TOKENS_PER_SECOND` and `RATE_LIMIT_BURST`.

### Proxy rotation

Set comma-separated proxy URLs:

```bash
CRAWLER_PROXY_URLS=http://proxy1:8080,http://proxy2:8080
```

### Retry and DLQ recovery

Failed crawls/parses publish to `argus.url.failed`. The **retry** service applies exponential backoff and republishes. Terminal failures go to `argus.dlq`; the **dlq-replay** service can recover them (capped at 3 replays).

### Incremental crawl

Recrawl stale URLs and skip unchanged content (content-hash comparison):

```bash
curl -X POST http://localhost:8001/jobs \
  -H "Content-Type: application/json" \
  -d '{"incremental": true, "allowed_domains": ["example.com"], "recrawl_stale_hours": 24}'
```

### Distributed workers

Scale workers horizontally via Kafka consumer groups:

```bash
docker compose up -d --scale crawler=3 --scale parser=2 --scale cleaner=2
```

## License

MIT
