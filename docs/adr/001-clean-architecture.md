# ADR 001: Clean Architecture Package Boundaries

## Status

Accepted

## Context

`argus-core` mixed models, parsing, search, scheduling, Redis, and MinIO in a flat package. Services became thin shells with logic scattered across workers and shared modules, making testing and interview-level reasoning harder.

## Decision

Introduce `domain/`, `application/`, `ports/`, and `infrastructure/` under `argus_core`. Legacy imports (`argus_core.models`, `argus_core.parser`) remain as compatibility re-exports during migration.

Entrypoints (`services/*/main.py`) become composition roots that wire concrete adapters to application services.

## Consequences

**Pros:** Testable use cases, clear dependency direction, easier to explain in interviews.

**Cons:** More files; incremental migration required to avoid big-bang rewrite.

## Alternatives Considered

- **Microservice-only boundaries:** Rejected; shared domain logic still needs a kernel.
- **Full rewrite:** Rejected; breaks working pipeline and delays reliability fixes.
