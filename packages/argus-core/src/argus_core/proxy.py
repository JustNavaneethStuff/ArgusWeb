"""Phase 2 scaffold: proxy rotation."""

from __future__ import annotations

from abc import ABC, abstractmethod


class ProxyRotator(ABC):
    @abstractmethod
    def next_proxy(self) -> str | None:
        """Return next proxy URL or None for direct connection."""


class NoOpProxyRotator(ProxyRotator):
    def next_proxy(self) -> str | None:
        return None


class RoundRobinProxyRotator(ProxyRotator):
    """Phase 2: round-robin proxy pool from env."""

    def __init__(self, proxies: list[str]) -> None:
        self._proxies = proxies
        self._index = 0

    def next_proxy(self) -> str | None:
        if not self._proxies:
            return None
        proxy = self._proxies[self._index % len(self._proxies)]
        self._index += 1
        return proxy
