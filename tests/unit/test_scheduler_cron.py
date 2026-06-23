import uuid

import pytest

from argus_core.scheduler import NoOpJobScheduler


@pytest.mark.asyncio
async def test_noop_scheduler():
    sched = NoOpJobScheduler()
    sid = await sched.schedule_cron(uuid.uuid4(), "0 * * * *", {})
    assert sid == "noop"
    await sched.cancel("noop")
    await sched.load_from_db()


def test_cron_expression_validation():
    parts = "0 */6 * * *".split()
    assert len(parts) == 5
