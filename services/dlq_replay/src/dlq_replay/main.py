"""Phase 2 stub: DLQ replay consumer."""

from __future__ import annotations


class DlqReplayConsumer:
    """Consumes argus.dlq and replays failed events with backoff (Phase 2)."""

    async def start(self) -> None:
        raise NotImplementedError("DLQ replay is planned for Phase 2")
