"""Tests for extraction engine and schema validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from argus_core.extraction.engine import ExtractionEngine
from argus_core.extraction.json_extractor import extract_from_json
from argus_core.parser import extract_page_data

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture
def sample_html() -> bytes:
    return (FIXTURES / "sample.html").read_bytes()


def test_extract_page_data_legacy_compat(sample_html: bytes):
    data = extract_page_data(sample_html, "https://example.com/page")
    assert "title" in data
    assert "extracted_links" in data
    assert isinstance(data["metadata"], dict)


def test_extraction_engine_html(sample_html: bytes):
    engine = ExtractionEngine()
    page = engine.extract(sample_html, "https://example.com/page")
    assert page.content_type == "text/html"
    assert page.title is not None


def test_json_extractor():
    payload = {
        "title": "API Page",
        "description": "From JSON",
        "items": [{"url": "https://example.com/a"}],
        "next": "https://example.com/page/2",
    }
    page = extract_from_json(payload)
    assert page.title == "API Page"
    assert "https://example.com/page/2" in page.extracted_links


def test_extracted_page_validates_links():
    engine = ExtractionEngine()
    page = engine.extract(b"<html><a href='https://example.com'>x</a></html>", "https://x.com")
    for link in page.extracted_links:
        assert link.startswith("http")
