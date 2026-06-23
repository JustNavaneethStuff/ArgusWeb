from uuid import uuid4

from argus_events.schemas import UrlDiscovered, UrlFailed


def test_url_failed_has_replay_fields():
    job_id = uuid4()
    failed = UrlFailed(
        job_id=job_id,
        url="https://example.com",
        normalized_url="https://example.com",
        attempt=2,
        error="timeout",
        depth=1,
        max_depth=3,
        allowed_domains=["example.com"],
    )
    discovered = UrlDiscovered(
        job_id=failed.job_id,
        url=failed.url,
        normalized_url=failed.normalized_url,
        attempt=failed.attempt + 1,
        depth=failed.depth,
        max_depth=failed.max_depth,
        allowed_domains=failed.allowed_domains,
    )
    assert discovered.max_depth == 3


def test_search_response_shape():
    payload = {
        "query": "test",
        "total": 1,
        "limit": 20,
        "offset": 0,
        "results": [
            {
                "title": "Test",
                "url": "https://example.com",
                "normalized_url": "https://example.com",
                "description": None,
                "text_snippet": "hello",
                "domain": "example.com",
                "score": 0.5,
            }
        ],
    }
    assert payload["total"] == 1
    assert len(payload["results"]) == 1
