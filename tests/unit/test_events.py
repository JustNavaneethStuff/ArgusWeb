from uuid import uuid4

from argus_events.schemas import HtmlFetched, PageParsed, UrlDiscovered


def test_url_discovered_round_trip():
    event = UrlDiscovered(
        job_id=uuid4(),
        url="https://example.com",
        normalized_url="https://example.com",
        max_depth=2,
        allowed_domains=["example.com"],
    )
    raw = event.model_dump_json()
    restored = UrlDiscovered.model_validate_json(raw)
    assert restored.normalized_url == event.normalized_url
    assert restored.max_depth == 2


def test_html_fetched_round_trip():
    event = HtmlFetched(
        job_id=uuid4(),
        url="https://example.com",
        normalized_url="https://example.com",
        storage_key="job/hash.html",
        checksum="abc",
        size_bytes=100,
    )
    restored = HtmlFetched.model_validate_json(event.model_dump_json())
    assert restored.storage_key == event.storage_key


def test_page_parsed_round_trip():
    event = PageParsed(
        job_id=uuid4(),
        url="https://example.com",
        normalized_url="https://example.com",
        storage_key="key",
        checksum="abc",
        title="Test",
        extracted_links=["https://example.com/a"],
    )
    restored = PageParsed.model_validate_json(event.model_dump_json())
    assert restored.title == "Test"
