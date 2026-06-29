"""Unit tests for idempotency service and processed event repository."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from argus_core.application.idempotency import IdempotencyService


@pytest.mark.asyncio
async def test_should_process_returns_true_on_first_claim():
    repo = AsyncMock()
    repo.try_claim = AsyncMock(return_value=True)
    service = IdempotencyService(repo)
    session = MagicMock()

    result = await service.should_process(session, uuid.uuid4(), "crawler")

    assert result is True
    repo.try_claim.assert_awaited_once()


@pytest.mark.asyncio
async def test_should_process_returns_false_on_duplicate():
    repo = AsyncMock()
    repo.try_claim = AsyncMock(return_value=False)
    service = IdempotencyService(repo)
    session = MagicMock()

    result = await service.should_process(session, uuid.uuid4(), "parser")

    assert result is False
