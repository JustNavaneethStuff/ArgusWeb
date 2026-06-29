"""Tests for crawl progress service."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from argus_core.application.crawl_progress import CrawlProgressService
from argus_core.domain.crawl import CrawlAttemptStatus


@pytest.mark.asyncio
async def test_mark_delegates_to_repository():
    repo = AsyncMock()
    service = CrawlProgressService(repo)
    session = AsyncMock()
    job_id = uuid.uuid4()

    await service.mark(session, job_id, "https://example.com", CrawlAttemptStatus.fetched)

    repo.transition.assert_awaited_once_with(
        session,
        job_id,
        "https://example.com",
        "fetched",
        error=None,
        checkpoint=None,
    )
