from __future__ import annotations

import hashlib
from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_url(url: str) -> str:
    """Normalize a URL for deduplication and canonical comparison."""
    parsed = urlparse(url.strip())

    scheme = (parsed.scheme or "http").lower()
    netloc = parsed.netloc.lower()

    if not netloc and parsed.path:
        # Handle URLs like "example.com/path"
        reparsed = urlparse(f"http://{url.strip()}")
        scheme = reparsed.scheme.lower()
        netloc = reparsed.netloc.lower()
        path = reparsed.path
        query = reparsed.query
    else:
        path = parsed.path
        query = parsed.query

    # Remove default port
    if ":" in netloc:
        host, port_str = netloc.rsplit(":", 1)
        try:
            port = int(port_str)
            if DEFAULT_PORTS.get(scheme) == port:
                netloc = host
        except ValueError:
            pass

    if not path:
        path = ""
    elif path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    # Sort query parameters
    if query:
        params = parse_qsl(query, keep_blank_values=True)
        query = urlencode(sorted(params))

    # Strip fragment
    normalized = urlunparse((scheme, netloc, path, "", query, ""))
    return normalized


def url_hash(normalized_url: str) -> str:
    """SHA-256 hash of normalized URL for dedup keys."""
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(normalize_url(url))
    netloc = parsed.netloc
    if ":" in netloc:
        return netloc.split(":")[0]
    return netloc


@lru_cache(maxsize=4096)
def is_same_domain(url: str, allowed_domains: tuple[str, ...]) -> bool:
    """Check if URL belongs to one of the allowed domains."""
    domain = extract_domain(url)
    return any(domain == d or domain.endswith(f".{d}") for d in allowed_domains)
