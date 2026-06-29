from __future__ import annotations

import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from argus_core.extraction.schemas import ExtractedPage, PaginationHint, StructuredMetadata
from argus_core.url_utils import normalize_url


def extract_json_ld(soup: BeautifulSoup) -> tuple[list[dict], list[str]]:
    items: list[dict] = []
    types: list[str] = []
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            items.extend(data)
        elif isinstance(data, dict):
            items.append(data)
        for obj in items:
            t = obj.get("@type")
            if isinstance(t, str):
                types.append(t)
            elif isinstance(t, list):
                types.extend(str(x) for x in t)
    return items, list(dict.fromkeys(types))


def extract_meta(soup: BeautifulSoup) -> StructuredMetadata:
    open_graph: dict[str, str] = {}
    meta_tags: dict[str, str] = {}
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "")
        name = meta.get("name", "")
        content = meta.get("content", "")
        if prop.startswith("og:") and content:
            open_graph[prop] = content.strip()
        elif name and content:
            meta_tags[name] = content.strip()
    json_ld, schema_types = extract_json_ld(soup)
    return StructuredMetadata(
        open_graph=open_graph,
        meta_tags=meta_tags,
        json_ld=json_ld,
        schema_types=schema_types,
    )


def extract_pagination(soup: BeautifulSoup, base_url: str) -> PaginationHint:
    next_url = None
    prev_url = None
    page_numbers: list[int] = []

    next_link = soup.find("link", rel="next")
    if next_link and next_link.get("href"):
        next_url = normalize_url(urljoin(base_url, next_link["href"]))

    prev_link = soup.find("link", rel="prev")
    if prev_link and prev_link.get("href"):
        prev_url = normalize_url(urljoin(base_url, prev_link["href"]))

    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(strip=True)
        if text.isdigit():
            page_numbers.append(int(text))

    is_infinite = bool(
        soup.find(attrs={"data-infinite-scroll": True})
        or soup.find(class_=re.compile(r"infinite[-_]?scroll", re.I))
    )

    return PaginationHint(
        next_url=next_url,
        prev_url=prev_url,
        page_numbers=sorted(set(page_numbers)),
        is_infinite_scroll=is_infinite,
    )


def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
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
    return links


def extract_from_html(html: bytes, base_url: str) -> ExtractedPage:
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

    metadata = extract_meta(soup)
    pagination = extract_pagination(soup, base_url)
    links = extract_links(soup, base_url)

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text_snippet = text[:500] if text else None

    return ExtractedPage(
        title=title,
        description=description,
        language=language,
        text_snippet=text_snippet,
        extracted_links=links,
        metadata=metadata,
        pagination=pagination,
        content_type="text/html",
    )
