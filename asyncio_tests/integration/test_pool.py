import asyncio
import json
import time
from copy import deepcopy

import pytest
from conftest import BASIC_CONFIG

from dispatcherd.testing.asyncio import adispatcher_service

SLEEP_METHOD = 'lambda: __import__("time").sleep(1.5)'
LIGHT_SLEEP_METHOD = 'lambda: __import__("time").sleep(0.03)'


@pytest.mark.asyncio
async def test_task_timeout(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    start_time = time.monotonic()

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await pg_message(json.dumps({'task': SLEEP_METHOD, 'timeout': 0.1}))
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
        await pg_message(json.dumps({'task': SLEEP_METHOD, 'timeout': 0.01 * i + 0.01, 'uuid': f'test_multiple_task_timeouts_{i}'}))
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
        await pg_message(
            json.dumps({'task': SLEEP_METHOD if (i % 2) else LIGHT_SLEEP_METHOD, 'timeout': 0.01 * (i % 2), 'uuid': f'test_multiple_task_timeouts_{i}'})
        )
    await asyncio.wait_for(clearing_task, timeout=3)

    delta = time.monotonic() - start_time

    assert delta < 1.0
    # half of the tasks should be finished, half should have been canceled
    assert apg_dispatcher.pool.canceled_count == 3


@pytest.mark.asyncio
async def test_workers_retire_when_exceeding_max_lifetime(pg_message):
    worker_lifetime = 1.0
    first_task_seconds = 2.0
    short_lived_config = deepcopy(BASIC_CONFIG)
    short_lived_config.setdefault('service', {})
    short_lived_config['service']['process_manager_cls'] = 'ProcessManager'
    short_lived_config['service'].setdefault('pool_kwargs', {})
    short_lived_config['service']['pool_kwargs'].update(
        {
            'min_workers': 1,
            'max_workers': 2,
            'scaledown_interval': 0.01,
            'worker_max_lifetime_seconds': worker_lifetime,
        }
    )

    async with adispatcher_service(short_lived_config) as dispatcher:
        pool = dispatcher.pool
        pool.events.work_cleared.clear()
        worker0 = pool.workers.get_by_id(0)
        if time.monotonic() - worker0.created_at >= worker_lifetime:
            pytest.skip('Worker lifetime expired before long-running task could be scheduled')

        await pg_message(json.dumps({'task': 'tests.data.methods.sleep_function', 'kwargs': {'seconds': first_task_seconds}, 'uuid': 'retirement-task-long'}))

        async def _wait_until_worker_age(min_age: float) -> None:
            while time.monotonic() - worker0.created_at < min_age:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_wait_until_worker_age(worker_lifetime), timeout=5)

        await pg_message(json.dumps({'task': 'tests.data.methods.sleep_function', 'kwargs': {'seconds': 0.05}, 'uuid': 'retirement-task-short'}))

        await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=5)

        async def _wait_for_worker(worker_id: int) -> None:
            while worker_id not in pool.workers:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_wait_for_worker(1), timeout=5)
        worker0_status = pool.workers.get_by_id(0).status if 0 in pool.workers else 'removed'
        worker1 = pool.workers.get_by_id(1)

        assert worker0_status != 'ready'
        assert worker1.finished_count == 1
        assert dispatcher.pool.finished_count == 2
