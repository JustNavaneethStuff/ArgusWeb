from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from argus_core.url_utils import normalize_url


def extract_page_data(html: bytes, base_url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    description = None
    desc_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if desc_tag and desc_tag.get("content"):
        description = desc_tag["content"].strip()
    else:
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            description = og_desc["content"].strip()

    html_tag = soup.find("html")
    language = html_tag.get("lang") if html_tag else None

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text_snippet = text[:500] if text else None

    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = normalize_url(urljoin(base_url, href))
        if absolute not in seen:
            seen.add(absolute)
            links.append(absolute)

    metadata: dict = {}
    for meta in soup.find_all("meta"):
        if meta.get("property", "").startswith("og:"):
            metadata[meta["property"]] = meta.get("content", "")
        elif meta.get("name"):
            metadata[meta["name"]] = meta.get("content", "")

    return {
        "title": title,
        "description": description,
        "language": language,
        "text_snippet": text_snippet,
        "extracted_links": links,
        "metadata": metadata,
    }


def filter_links_by_domain(links: list[str], allowed_domains: list[str]) -> list[str]:
    result = []
    for link in links:
        domain = urlparse(link).netloc.split(":")[0]
        if any(domain == d or domain.endswith(f".{d}") for d in allowed_domains):
            result.append(link)
    return result
