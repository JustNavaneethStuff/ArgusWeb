from argus_core.url_utils import extract_domain, normalize_url, url_hash


def test_normalize_strips_fragment():
    assert normalize_url("https://Example.com/path#section") == "https://example.com/path"


def test_normalize_sorts_query_params():
    assert normalize_url("https://example.com?b=2&a=1") == "https://example.com?a=1&b=2"


def test_normalize_removes_default_port():
    assert normalize_url("https://example.com:443/page") == "https://example.com/page"


def test_normalize_trailing_slash():
    assert normalize_url("https://example.com/path/") == "https://example.com/path"


def test_url_hash_deterministic():
    url = normalize_url("https://example.com")
    assert url_hash(url) == url_hash(url)


def test_extract_domain():
    assert extract_domain("https://www.example.com:8080/path") == "www.example.com"
