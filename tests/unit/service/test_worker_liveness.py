import asyncio
import time
from unittest import mock
from types import SimpleNamespace

import pytest

from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects


@pytest.mark.asyncio
async def test_detect_unexpectedly_dead_worker(test_settings, caplog):
    """
    Verify that if a worker dies unexpectedly while processing a task,
    it is marked as 'error', its task is canceled, and appropriate error logs are produced.
    """
    # Create a pool with one worker and start it
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=5, shared=SharedAsyncObjects())
    await pool.start_working(SimpleNamespace())
    await pool.events.workers_ready.wait()

    # Get the ready worker and assign a task
    worker = list(pool.workers)[0]
    worker.current_task = {'uuid': 'test-task-123'}
    worker_pid = worker.process.pid
    assert worker_pid is not None, "Worker process PID should not be None"

    # Directly kill the worker's process using kill(), which sends SIGKILL
    with caplog.at_level("ERROR"):
        worker.process.kill()
        await asyncio.sleep(0.1)  # Allow time for the kill to be registered
        await pool.manage_old_workers()

    # Verify that the worker's status is updated and the cancellation counter incremented
    assert worker.status == 'error'
    assert worker.retired_at is not None
    assert pool.canceled_count == 1
    assert "died unexpectedly" in caplog.text
    assert "test-task-123" in caplog.text

    # Clean up by shutting down the pool (override cancel to prevent errors)
    for w in pool.workers:
        w.cancel = lambda: None
    pool.shared.exit_event.set()
    await pool.shutdown()


@pytest.mark.asyncio
async def test_manage_dead_worker_removal(test_settings):
    """
    Verify that a worker marked as dead is removed from the pool after the removal wait period.
    """
    pm = ProcessManager(settings=test_settings)
    # Set a very short removal wait time for the test.
    pool = WorkerPool(pm, min_workers=1, max_workers=3, worker_removal_wait=0.1, shared=SharedAsyncObjects())
    await pool.up()
    worker = pool.workers.get_by_id(0)
    worker.status = 'error'
    # Set retired_at to a time sufficiently in the past.
    worker.retired_at = time.monotonic() - 1.0

    await pool.manage_old_workers()
    # Allow a brief moment for asynchronous operations to complete.
    await asyncio.sleep(0.01)

    # Assert that the worker has been removed from the pool == the pool is now empty.
    assert len(pool.workers) == 0

    pool.shared.exit_event.set()
    await pool.shutdown()


@pytest.mark.asyncio
async def test_dead_worker_with_no_task(test_settings):
    """
    Verify that a dead worker which is not running any task is marked as error,
    without increasing the canceled task count.
    """
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=3, shared=SharedAsyncObjects())
    await pool.up()
    worker = pool.workers.get_by_id(0)

    # Set the status to 'ready' instead of waiting for a workers_ready event (which may never fire)
    worker.status = 'ready'
    worker.current_task = None

    initial_canceled = pool.canceled_count

    with mock.patch.object(worker.process, 'is_alive', return_value=False):
        await pool.manage_old_workers()

    # Assert that the worker status is marked as error.
    assert worker.status == 'error'
    # And the canceled counter remains unchanged since there was no running task.
    assert pool.canceled_count == initial_canceled

    pool.shared.exit_event.set()
    await pool.shutdown()
