import time
import asyncio
from unittest import mock

import pytest

from dispatcher.service.pool import WorkerPool
from dispatcher.service.process import ProcessManager


@pytest.mark.asyncio
async def test_detect_unexpectedly_dead_worker(test_settings, caplog):
    """
    Verify that when a worker that is processing a task dies unexpectedly,
    it is marked with an error status, its task is canceled, and proper logging occurs.
    """
    # Setup: create a pool with one worker and set its state as ready with a running task.
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=2, max_workers=5)
    await pool.up()
    worker = pool.workers[0]
    worker.status = 'ready'
    worker.current_task = {'uuid': 'test-task-123'}

    # Simulate unexpected process death by forcing is_alive to return False.
    with caplog.at_level("DEBUG"):
        with mock.patch.object(worker.process, 'is_alive', return_value=False):
            await pool.manage_old_workers()

    # Assert the worker status is updated and the task cancellation counter is incremented.
    assert worker.status == 'error'
    assert hasattr(worker, 'retired_at') and worker.retired_at is not None
    assert pool.canceled_count == 1

    # Verify that an error message was logged.
    assert "Worker" in caplog.text


@pytest.mark.asyncio
async def test_manage_dead_worker_removal(test_settings):
    """
    Verify that a worker marked as dead is removed from the pool after the removal wait period.
    """
    pm = ProcessManager(settings=test_settings)
    # Set a very short removal wait time for the test.
    pool = WorkerPool(pm, min_workers=1, max_workers=3, worker_removal_wait=0.1)
    await pool.up()
    worker = pool.workers[0]
    worker.status = 'error'
    # Set retired_at to a time sufficiently in the past.
    worker.retired_at = time.monotonic() - 1.0

    await pool.manage_old_workers()
    # Allow a brief moment for asynchronous operations to complete.
    await asyncio.sleep(0.01)

    # Assert that the worker has been removed from the pool == the pool is now empty.
    assert len(pool.workers) == 0


@pytest.mark.asyncio
async def test_dead_worker_with_no_task(test_settings):
    """
    Verify that a dead worker which is not running any task is marked as error,
    without increasing the canceled task count.
    """
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=3)
    await pool.up()
    worker = pool.workers[0]
    worker.status = 'ready'
    worker.current_task = None

    initial_canceled = pool.canceled_count

    with mock.patch.object(worker.process, 'is_alive', return_value=False):
        await pool.manage_old_workers()

    # Assert that the worker status is marked as error.
    assert worker.status == 'error'
    # And the canceled counter remains unchanged since there was no running task.
    assert pool.canceled_count == initial_canceled
