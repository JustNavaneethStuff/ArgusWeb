from __future__ import annotations

from urllib.parse import urlparse

from argus_core.extraction.engine import ExtractionEngine
from argus_core.url_utils import normalize_url

_engine = ExtractionEngine()


def extract_page_data(html: bytes, base_url: str) -> dict:
    """Backward-compatible wrapper returning legacy dict shape."""
    page = _engine.extract(html, base_url, content_type="text/html")
    return page.to_legacy_dict()


def filter_links_by_domain(links: list[str], allowed_domains: list[str]) -> list[str]:
    result = []
    for link in links:
        domain = urlparse(link).netloc.split(":")[0]
        if any(domain == d or domain.endswith(f".{d}") for d in allowed_domains):
            result.append(link)
    return result


def extract_links_only(html: bytes, base_url: str) -> list[str]:
    """Lightweight link discovery for crawler stage."""
    page = _engine.extract(html, base_url)
    return page.extracted_links
