import time
import asyncio
import json

import pytest

from tests.data import methods as test_methods

SLEEP_METHOD = 'lambda: __import__("time").sleep(1.5)'


@pytest.mark.asyncio
async def test_task_timeout(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    start_time = time.monotonic_ns()

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await pg_message(json.dumps({
        'task': SLEEP_METHOD,
        'timeout': 0.1
    }))
    await asyncio.wait_for(clearing_task, timeout=3)

    delta = time.monotonic_ns() - start_time

    assert delta < 1.0  # proves task did not run to completion
    assert apg_dispatcher.pool.canceled_count == 1
