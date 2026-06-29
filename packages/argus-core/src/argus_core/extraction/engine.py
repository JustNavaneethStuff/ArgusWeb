from __future__ import annotations

from argus_core.extraction.html_extractor import extract_from_html
from argus_core.extraction.json_extractor import extract_from_json
from argus_core.extraction.schemas import ExtractedPage


class ExtractionEngine:
    """Routes content to typed extractors and validates output."""

    def extract(
        self,
        content: bytes,
        base_url: str,
        content_type: str = "text/html",
    ) -> ExtractedPage:
        if "json" in content_type.lower():
            import json

            data = json.loads(content.decode("utf-8"))
            page = extract_from_json(data, base_url)
        else:
            page = extract_from_html(content, base_url)
        return page
