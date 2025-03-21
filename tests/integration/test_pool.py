import asyncio
import json
import time

import pytest

from tests.data import methods as test_methods

SLEEP_METHOD = 'lambda: __import__("time").sleep(1.5)'
LIGHT_SLEEP_METHOD = 'lambda: __import__("time").sleep(0.03)'


@pytest.mark.asyncio
async def test_task_timeout(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    start_time = time.monotonic()

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await pg_message(json.dumps({
        'task': SLEEP_METHOD,
        'timeout': 0.1
    }))
    await asyncio.wait_for(clearing_task, timeout=3)

    delta = time.monotonic() - start_time

    assert delta < 1.0  # proves task did not run to completion
    assert apg_dispatcher.pool.canceled_count == 1


@pytest.mark.asyncio
async def test_multiple_task_timeouts(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    start_time = time.monotonic()

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    for i in range(5):
        await pg_message(json.dumps({
            'task': SLEEP_METHOD,
            'timeout': 0.01*i + 0.01,
            'uuid': f'test_multiple_task_timeouts_{i}'
        }))
    await asyncio.wait_for(clearing_task, timeout=3)

    delta = time.monotonic() - start_time

    assert delta < 1.0  # proves task did not run to completion
    assert apg_dispatcher.pool.canceled_count == 5


@pytest.mark.asyncio
async def test_mixed_timeouts_non_timeouts(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    start_time = time.monotonic()

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    for i in range(6):
        await pg_message(json.dumps({
            'task': SLEEP_METHOD if (i % 2) else LIGHT_SLEEP_METHOD,
            'timeout': 0.01 * (i % 2),
            'uuid': f'test_multiple_task_timeouts_{i}'
        }))
    await asyncio.wait_for(clearing_task, timeout=3)

    delta = time.monotonic() - start_time

    assert delta < 1.0
    # half of the tasks should be finished, half should have been canceled
    assert apg_dispatcher.pool.canceled_count == 3
