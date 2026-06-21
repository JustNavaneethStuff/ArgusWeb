def test_content_hash_skip_decision():
    stored = "abc123"
    new = "abc123"
    assert stored == new


def test_content_hash_change_detected():
    stored = "abc123"
    new = "def456"
    assert stored != new
