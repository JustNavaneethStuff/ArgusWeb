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
services/api                 Analytics + health API
frontend/dashboard           React product UI
```

## Phases

- **Phase 1** (current): Async crawl pipeline, robots.txt, dedup, HTML storage, metadata extraction
- **Phase 2**: Rate limiting, proxies, retry/DLQ, incremental crawl
- **Phase 3**: Search API, cron scheduling, full dashboard, alerting

## License

MIT
