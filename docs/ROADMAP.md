# Roadmap

## Completed (Production Platform v1)

- [x] Clean Architecture package boundaries
- [x] Manual Kafka commit + bounded consumption
- [x] Idempotent event processing (`processed_events`)
- [x] Per-job URL accounting (`crawl_job_urls`)
- [x] Typed extraction engine (HTML, JSON, metadata, pagination)
- [x] DatabaseManager lifecycle
- [x] Trace propagation via Kafka headers
- [x] CI: lint, coverage, Docker build, security audit
- [x] Architecture + interview documentation

## Next

- [ ] API v1 versioning with auth (JWT/API keys)
- [ ] OpenSearch for full-text search at scale
- [ ] Kubernetes Helm charts
- [ ] Scheduler leader election for multi-replica cron
- [ ] Real testcontainers e2e in CI
- [ ] Grafana consumer lag dashboards
- [ ] HTTP fetcher fast path for static HTML
