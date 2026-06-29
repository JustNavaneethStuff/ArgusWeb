from __future__ import annotations

from argus_core.extraction.schemas import ExtractedPage, StructuredMetadata


def extract_from_json(data: dict | list, base_url: str = "") -> ExtractedPage:
    """Extract structured fields from JSON API responses."""
    if isinstance(data, list):
        data = {"items": data}

    title = None
    description = None
    links: list[str] = []

    if isinstance(data.get("title"), str):
        title = data["title"]
    elif isinstance(data.get("name"), str):
        title = data["name"]

    if isinstance(data.get("description"), str):
        description = data["description"]

    for key in ("url", "href", "link", "next", "next_page"):
        val = data.get(key)
        if isinstance(val, str) and val.startswith("http"):
            links.append(val)

    items = data.get("items") or data.get("results") or data.get("data")
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                for key in ("url", "href", "link"):
                    val = item.get(key)
                    if isinstance(val, str) and val.startswith("http"):
                        links.append(val)

    return ExtractedPage(
        title=title,
        description=description,
        text_snippet=str(data)[:500] if data else None,
        extracted_links=list(dict.fromkeys(links)),
        metadata=StructuredMetadata(meta_tags={"source": "json"}),
        content_type="application/json",
    )
