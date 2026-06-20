from __future__ import annotations

import asyncio
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import httpx

from argus_core.settings import Settings


class RobotsChecker:
    """Async robots.txt checker with in-memory cache."""

    def __init__(self, settings: Settings) -> None:
        self._user_agent = settings.crawler_user_agent
        self._cache: dict[str, robotparser.RobotFileParser] = {}
        self._lock = asyncio.Lock()

    def _robots_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    async def _fetch_parser(self, url: str) -> robotparser.RobotFileParser:
        domain = urlparse(url).netloc
        async with self._lock:
            if domain in self._cache:
                return self._cache[domain]

        robots_url = self._robots_url(url)
        parser = robotparser.RobotFileParser()
        parser.set_url(robots_url)

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(robots_url, headers={"User-Agent": self._user_agent})
                if response.status_code == 200:
                    parser.parse(response.text.splitlines())
                else:
                    parser.parse([])
        except httpx.HTTPError:
            parser.parse([])

        async with self._lock:
            self._cache[domain] = parser
        return parser

    async def is_allowed(self, url: str) -> bool:
        parser = await self._fetch_parser(url)
        return parser.can_fetch(self._user_agent, url)

    @staticmethod
    def sitemap_urls(base_url: str) -> list[str]:
        parsed = urlparse(base_url)
        return [urljoin(f"{parsed.scheme}://{parsed.netloc}", "/sitemap.xml")]
