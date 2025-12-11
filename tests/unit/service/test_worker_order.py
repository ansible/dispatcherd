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



@pytest.mark.asyncio
async def test_ready_queue_ignores_removed_worker():
    """Ready queue entries pointing to removed workers should be ignored gracefully."""
    from unittest.mock import MagicMock
    from dispatcherd.service.pool import WorkerData, PoolWorker

    worker_data = WorkerData()

    mock_process = MagicMock()
    mock_process.message_queue = MagicMock()
    mock_process.start = MagicMock()
    mock_process.join = MagicMock()
    mock_process.is_alive.return_value = False
    mock_process.exitcode.return_value = 0
    mock_process.kill = MagicMock()
    mock_process.pid = 123

    worker = PoolWorker(worker_id=0, process=mock_process)
    async with worker.lock:
        worker.status = 'ready'
        worker.current_task = None

    await worker_data.add_worker(worker)
    worker_data.enqueue_ready_worker(worker)
    await worker_data.remove_by_id(worker.worker_id)

    reserved = await worker_data.reserve_ready_worker({'task': 'demo', 'uuid': 'ignored'})
    assert reserved is None
