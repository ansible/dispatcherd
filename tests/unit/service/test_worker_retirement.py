import time
from types import SimpleNamespace

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.pool import WorkerPool


class DummyProcessManager:
    """Minimal stub that satisfies WorkerPool without touching multiprocessing."""

    def __init__(self):
        self.finished_queue = SimpleNamespace(put=lambda *args, **kwargs: None)

    def create_process(self, *args, **kwargs):
        raise AssertionError("DummyProcessManager should not create processes in this test")

    async def read_finished(self):
        raise AssertionError("No worker processes should be running in this test")

    def shutdown(self):
        pass


class FakeProcess:
    def __init__(self):
        self.message_queue = SimpleNamespace(put=lambda *args, **kwargs: None, close=lambda: None)
        self.pid = 1234

    def is_alive(self):
        return True

    def kill(self):
        pass

    def exitcode(self):
        return 0


class FakeWorker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.status = 'ready'
        self.current_task = {'uuid': 'long-task'}
        self.created_at = time.monotonic() - 10.0
        self.stopping_at = None
        self.process = FakeProcess()
        self.signal_stop_calls: list[bool] = []
        self.stop_called = False
        self.retired_at = None
        self.is_active_cancel = False

    @property
    def expected_alive(self) -> bool:
        return self.status in ('starting', 'ready')

    async def signal_stop(self, *, cancel_if_busy: bool = True):
        self.signal_stop_calls.append(cancel_if_busy)
        self.status = 'stopping'
        self.stopping_at = time.monotonic()

    async def stop(self):
        self.stop_called = True


@pytest.mark.asyncio
async def test_retiring_worker_not_force_stopped_before_finishing_task():
    """Retiring workers should not be forcibly stopped while still running their task."""
    shared = SharedAsyncObjects()
    pool = WorkerPool(
        process_manager=DummyProcessManager(),
        shared=shared,
        worker_stop_wait=0.05,
        worker_max_lifetime_seconds=0.0,
    )

    worker = FakeWorker(worker_id=0)
    pool.workers.add_worker(worker)

    # Retirement should request a graceful stop without canceling the running task.
    await pool.retire_aged_workers()
    assert worker.status == 'stopping'
    assert worker.signal_stop_calls == [False]

    # Simulate the worker running longer than worker_stop_wait while still busy.
    worker.current_task = {'uuid': 'still-running'}
    worker.stopping_at = time.monotonic() - 1.0

    await pool.manage_old_workers()

    # Existing logic incorrectly forces a stop, which should not happen for retiring workers.
    assert worker.stop_called is False
