import pytest

from argus_core.url_utils import normalize_url, url_hash


@pytest.mark.integration
def test_dedup_logic():
    """Verify URL normalization produces identical hashes for duplicate URLs."""
    urls = [
        "https://example.com/page",
        "https://example.com/page/",
        "https://example.com/page#fragment",
    ]
    hashes = {url_hash(normalize_url(u)) for u in urls}
    assert len(hashes) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_event_pipeline_schema_compatibility():
    """Integration placeholder: validates cross-service event contract."""
    from uuid import uuid4

    from argus_events.schemas import HtmlFetched, PageParsed, UrlDiscovered

    job_id = uuid4()
    discovered = UrlDiscovered(
        job_id=job_id,
        url="https://example.com",
        normalized_url="https://example.com",
        max_depth=1,
        allowed_domains=["example.com"],
    )
    fetched = HtmlFetched(
        job_id=job_id,
        url=discovered.url,
        normalized_url=discovered.normalized_url,
        storage_key=f"{job_id}/hash.html",
        checksum="deadbeef",
        size_bytes=512,
        depth=0,
        max_depth=1,
        allowed_domains=["example.com"],
    )
    parsed = PageParsed(
        job_id=job_id,
        url=fetched.url,
        normalized_url=fetched.normalized_url,
        storage_key=fetched.storage_key,
        checksum=fetched.checksum,
        title="Example",
        depth=0,
    )
    assert parsed.job_id == discovered.job_id
