from uuid import uuid4

from argus_events.schemas import UrlDiscovered, UrlFailed


def test_url_failed_to_discovered_replay_fields():
    job_id = uuid4()
    failed = UrlFailed(
        job_id=job_id,
        url="https://example.com",
        normalized_url="https://example.com",
        attempt=2,
        error="timeout",
        stage="crawler",
        depth=1,
        max_depth=3,
        allowed_domains=["example.com"],
        force_recrawl=True,
    )
    discovered = UrlDiscovered(
        job_id=failed.job_id,
        url=failed.url,
        normalized_url=failed.normalized_url,
        attempt=failed.attempt + 1,
        depth=failed.depth,
        max_depth=failed.max_depth,
        allowed_domains=failed.allowed_domains,
        force_recrawl=failed.force_recrawl,
    )
    assert discovered.attempt == 3
    assert discovered.force_recrawl is True


def test_incremental_job_request_fields():
    payload = {
        "incremental": True,
        "allowed_domains": ["example.com"],
        "recrawl_stale_hours": 48,
        "seed_urls": [],
    }
    assert payload["incremental"] is True
    assert payload["allowed_domains"] == ["example.com"]
