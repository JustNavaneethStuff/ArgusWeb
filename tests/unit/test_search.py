from dataclasses import asdict

from argus_core.search import SearchResult


def test_search_result_dataclass():
    result = SearchResult(
        title="Test",
        url="https://example.com",
        normalized_url="https://example.com",
        description="desc",
        text_snippet="snippet",
        domain="example.com",
        score=0.85,
    )
    data = asdict(result)
    assert data["title"] == "Test"
    assert data["score"] == 0.85
