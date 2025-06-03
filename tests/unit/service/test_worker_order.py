import asyncio
from typing import AsyncIterator

import pytest
import pytest_asyncio

from dispatcherd.protocols import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service


@pytest.fixture(scope='session')
def order_config():
    return {
        "version": 2,
        "service": {
            "pool_kwargs": {"min_workers": 2, "max_workers": 2},
            "main_kwargs": {"node_id": "order-test"},
        },
    }


@pytest_asyncio.fixture
async def aorder_dispatcher(order_config) -> AsyncIterator[DispatcherMain]:
    async with adispatcher_service(order_config) as dispatcher:
        yield dispatcher


@pytest.mark.asyncio
async def test_workers_reorder_and_dispatch_longest_idle(aorder_dispatcher):
    pool = aorder_dispatcher.pool
    assert list(pool.workers.workers.keys()) == [0, 1]

    pool.events.work_cleared.clear()
    await aorder_dispatcher.process_message({
        "task": "tests.data.methods.sleep_function",
        "kwargs": {"seconds": 0.1},
        "uuid": "t1",
    })
    await aorder_dispatcher.process_message({
        "task": "tests.data.methods.sleep_function",
        "kwargs": {"seconds": 0.05},
        "uuid": "t2",
    })
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    assert list(pool.workers.workers.keys()) == [1, 0]

    pool.events.work_cleared.clear()
    await aorder_dispatcher.process_message({
        "task": "tests.data.methods.sleep_function",
        "kwargs": {"seconds": 0.01},
        "uuid": "t3",
    })
    await asyncio.sleep(0.01)
    assert pool.workers.get_by_id(1).current_task["uuid"] == "t3"
    assert pool.workers.get_by_id(0).current_task is None
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    pool.events.work_cleared.clear()
    await aorder_dispatcher.process_message({
        "task": "tests.data.methods.sleep_function",
        "kwargs": {"seconds": 0.01},
        "uuid": "t4",
    })
    await asyncio.sleep(0.01)
    assert pool.workers.get_by_id(0).current_task["uuid"] == "t4"
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    assert list(pool.workers.workers.keys()) == [1, 0]
