import pytest

from argus_core.proxy import RoundRobinProxyRotator
from argus_core.retry import ExponentialBackoffRetry


def test_round_robin_proxy_rotation():
    rotator = RoundRobinProxyRotator(["http://a:1", "http://b:2"])
    assert rotator.next_proxy() == "http://a:1"
    assert rotator.next_proxy() == "http://b:2"
    assert rotator.next_proxy() == "http://a:1"


def test_round_robin_empty():
    rotator = RoundRobinProxyRotator([])
    assert rotator.next_proxy() is None


def test_retry_allows_under_max():
    strategy = ExponentialBackoffRetry(max_attempts=5, base_delay=1.0)
    decision = strategy.decide(3, "timeout")
    assert decision.should_retry is True
    assert decision.delay_seconds == 4.0


def test_retry_stops_at_max():
    strategy = ExponentialBackoffRetry(max_attempts=5, base_delay=1.0)
    decision = strategy.decide(5, "timeout")
    assert decision.should_retry is False
