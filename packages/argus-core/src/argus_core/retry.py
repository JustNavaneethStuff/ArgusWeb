"""Phase 2 scaffold: retry strategy."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class RetryDecision:
    should_retry: bool
    delay_seconds: float
    reason: str = ""


class RetryStrategy(ABC):
    @abstractmethod
    def decide(self, attempt: int, error: str) -> RetryDecision:
        """Decide whether to retry and how long to wait."""


class ExponentialBackoffRetry(RetryStrategy):
    def __init__(self, max_attempts: int = 5, base_delay: float = 1.0) -> None:
        self._max_attempts = max_attempts
        self._base_delay = base_delay

    def decide(self, attempt: int, error: str) -> RetryDecision:
        if attempt >= self._max_attempts:
            return RetryDecision(False, 0, f"max attempts reached: {error}")
        delay = self._base_delay * (2 ** (attempt - 1))
        return RetryDecision(True, delay, error)
