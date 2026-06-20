from pathlib import Path

from argus_core.parser import extract_page_data, filter_links_by_domain

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_extract_page_data():
    html = (FIXTURES / "sample.html").read_bytes()
    data = extract_page_data(html, "https://example.com/page")

    assert data["title"] == "Argus Test Page"
    assert data["description"] == "A sample page for Argus parser tests"
    assert data["language"] == "en"
    assert "sample paragraph" in (data["text_snippet"] or "")
    assert "https://example.com/about" in data["extracted_links"]
    assert "https://example.com/contact" in data["extracted_links"]


def test_filter_links_by_domain():
    links = [
        "https://example.com/a",
        "https://other.com/b",
        "https://sub.example.com/c",
    ]
    filtered = filter_links_by_domain(links, ["example.com"])
    assert filtered == [
        "https://example.com/a",
        "https://sub.example.com/c",
    ]
