import pytest

from argus_core.rate_limit import RedisTokenBucketRateLimiter


class FakeRedis:
    def __init__(self):
        self.scripts = {}

    def register_script(self, script):
        self.scripts["bucket"] = script

        class ScriptRunner:
            async def __call__(_self, keys=None, args=None):
                return [1, 0]

        return ScriptRunner()


@pytest.mark.asyncio
async def test_rate_limiter_acquire():
    limiter = RedisTokenBucketRateLimiter(FakeRedis(), tokens_per_second=1.0, burst=5)
    await limiter.acquire("example.com")
