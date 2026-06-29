from __future__ import annotations

from dataclasses import dataclass

from argus_core.application.crawl_progress import CrawlProgressService
from argus_core.application.idempotency import IdempotencyService
from argus_core.infrastructure.persistence.repositories import (
    SqlAlchemyCrawlJobUrlRepository,
    SqlAlchemyProcessedEventRepository,
)
from argus_core.settings import Settings, get_settings


@dataclass
class WorkerDependencies:
    """Composition root for pipeline workers."""

    settings: Settings
    idempotency: IdempotencyService
    crawl_progress: CrawlProgressService


def build_worker_dependencies(settings: Settings | None = None) -> WorkerDependencies:
    settings = settings or get_settings()
    processed_repo = SqlAlchemyProcessedEventRepository()
    crawl_repo = SqlAlchemyCrawlJobUrlRepository()
    return WorkerDependencies(
        settings=settings,
        idempotency=IdempotencyService(processed_repo),
        crawl_progress=CrawlProgressService(crawl_repo),
    )
