"""Phase 3 scaffold: job scheduling."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable


class JobScheduler(ABC):
    @abstractmethod
    def schedule_cron(self, cron: str, callback: Callable[[], None]) -> str:
        """Schedule a recurring job. Returns job id."""

    @abstractmethod
    def cancel(self, job_id: str) -> None:
        """Cancel a scheduled job."""


class NoOpJobScheduler(JobScheduler):
    def schedule_cron(self, cron: str, callback: Callable[[], None]) -> str:
        return "noop"

    def cancel(self, job_id: str) -> None:
        return None
