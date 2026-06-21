from argus_core.retry import ExponentialBackoffRetry


def test_exponential_backoff_delays():
    strategy = ExponentialBackoffRetry(max_attempts=5, base_delay=2.0)
    assert strategy.decide(1, "err").delay_seconds == 2.0
    assert strategy.decide(2, "err").delay_seconds == 4.0
    assert strategy.decide(3, "err").delay_seconds == 8.0


def test_first_attempt_retries():
    strategy = ExponentialBackoffRetry(max_attempts=3)
    assert strategy.decide(1, "network").should_retry is True
