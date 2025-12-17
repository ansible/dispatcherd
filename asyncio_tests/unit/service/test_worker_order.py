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
    await aorder_dispatcher.process_message(
        {
            "task": "tests.data.methods.sleep_function",
            "kwargs": {"seconds": 0.1},
            "uuid": "t1",
        }
    )
    await aorder_dispatcher.process_message(
        {
            "task": "tests.data.methods.sleep_function",
            "kwargs": {"seconds": 0.05},
            "uuid": "t2",
        }
    )
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    assert list(pool.workers.workers.keys()) == [1, 0]

    pool.events.work_cleared.clear()
    await aorder_dispatcher.process_message(
        {
            "task": "tests.data.methods.sleep_function",
            "kwargs": {"seconds": 0.01},
            "uuid": "t3",
        }
    )
    await asyncio.sleep(0.01)
    assert pool.workers.get_by_id(1).current_task["uuid"] == "t3"
    assert pool.workers.get_by_id(0).current_task is None
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    pool.events.work_cleared.clear()
    await aorder_dispatcher.process_message(
        {
            "task": "tests.data.methods.sleep_function",
            "kwargs": {"seconds": 0.01},
            "uuid": "t4",
        }
    )
    await asyncio.sleep(0.01)
    assert pool.workers.get_by_id(0).current_task["uuid"] == "t4"
    await asyncio.wait_for(pool.events.work_cleared.wait(), timeout=1)

    assert list(pool.workers.workers.keys()) == [1, 0]


@pytest.mark.asyncio
async def test_process_finished_with_removed_worker():
    """Test that process_finished handles KeyError gracefully when worker has been removed

    This simulates the race condition where a worker finishes a task, but has been
    removed from self.workers before process_finished is called.
    """
    from unittest.mock import MagicMock, patch

    from dispatcherd.service.pool import PoolWorker, WorkerData

    # Create a minimal WorkerData instance
    worker_data = WorkerData()

    # Create a mock worker
    mock_process = MagicMock()
    worker = PoolWorker(worker_id=0, process=mock_process)
    worker.current_task = {"uuid": "test-uuid", "task": "test.task"}
    worker.finished_count = 0

    # Add worker then remove it (simulating the race condition)
    worker_data.add_worker(worker)
    worker_data.remove_by_id(0)
    assert 0 not in worker_data

    # Mock the logger to verify the warning is logged
    with patch('dispatcherd.service.pool.logger') as mock_logger:
        # Call move_to_end on the removed worker - should not raise KeyError
        worker_data.move_to_end(0)

        # Verify the warning was logged
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args[0][0]
        assert "Attempted to move worker_id=0 to end, but worker was already removed" in warning_call
