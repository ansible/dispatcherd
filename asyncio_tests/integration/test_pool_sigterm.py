import asyncio
import os
import signal
import time
from unittest import mock
import pytest
from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects


@pytest.mark.asyncio
async def test_pool_shutdown_with_worker_sigterm(test_settings):
    """Test that worker pool handles worker SIGTERM correctly and shuts down quickly"""
    shared = SharedAsyncObjects()
    process_manager = ProcessManager(settings=test_settings)
    pool = WorkerPool(process_manager, shared, min_workers=1)
    # Start the pool
    await pool.start_working(None)
    # Wait for workers to be ready
    await pool.events.workers_ready.wait()
    # Get the worker PID
    worker = next(iter(pool.workers))
    worker_pid = worker.process.pid
    assert worker_pid is not None
    # Send SIGTERM to the worker only
    os.kill(worker_pid, signal.SIGTERM)
    # Set the shared exit_event before shutdown
    shared.exit_event.set()
    # Start shutdown and measure time
    start_time = time.monotonic()
    await pool.shutdown()
    shutdown_time = time.monotonic() - start_time
    # Verify shutdown completed quickly (under 3 seconds)
    assert shutdown_time < 3.0, f"Shutdown took {shutdown_time:.2f} seconds, expected under 3 seconds"
    # Verify all workers are stopped
    assert not any(worker.process.is_alive() for worker in pool.workers)
    # Verify worker status is 'error' (indicating it did not send a stop message)
    assert worker.status == 'error', f"Worker status is {worker.status}, expected 'error'"


@pytest.mark.asyncio
async def test_worker_status_after_sigterm(test_settings):
    """Test that a worker's status is set to 'error' when it exits due to SIGTERM without sending a shutdown message"""
    shared = SharedAsyncObjects()
    process_manager = ProcessManager(settings=test_settings)
    pool = WorkerPool(process_manager, shared, min_workers=1)
    # Start the pool
    await pool.start_working(None)
    # Wait for workers to be ready
    await pool.events.workers_ready.wait()
    # Get the worker
    worker = next(iter(pool.workers))
    worker_pid = worker.process.pid
    assert worker_pid is not None
    # Send SIGTERM to the worker
    os.kill(worker_pid, signal.SIGTERM)
    # Call stop on the worker to update its status
    await worker.stop()
    # Check worker status after stop
    assert worker.status == 'error', f"Worker status is {worker.status}, expected 'error' after stop"
    # Verify the worker is not alive
    assert not worker.process.is_alive(), "Worker process should be dead after SIGTERM"
    # Set the shared exit_event before shutdown
    shared.exit_event.set()
    # Shutdown the pool
    await pool.shutdown()
