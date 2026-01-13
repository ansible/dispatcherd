import asyncio
import logging
import multiprocessing
import time
from typing import Callable
from unittest import mock

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.main import DispatcherMain
from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager


class _InstrumentedQueueWrapper:
    """Wrap a real multiprocessing.Queue so we know when get() started."""

    def __init__(self, queue, loop: asyncio.AbstractEventLoop):
        self._queue = queue
        self._loop = loop
        self._reader_waiting = asyncio.Event()

    def get(self, *args, **kwargs):
        self._loop.call_soon_threadsafe(self._reader_waiting.set)
        return self._queue.get(*args, **kwargs)

    def close(self) -> None:
        return self._queue.close()

    def put(self, *args, **kwargs):
        return self._queue.put(*args, **kwargs)

    async def wait_for_reader(self) -> None:
        await self._reader_waiting.wait()


@pytest.fixture
def pool_factory(test_settings) -> Callable[..., WorkerPool]:
    def _factory(**kwargs_overrides) -> WorkerPool:
        pm = ProcessManager(settings=test_settings)
        kwargs = dict(process_manager=pm, min_workers=5, max_workers=5, shared=SharedAsyncObjects())
        kwargs.update(kwargs_overrides)
        pool = WorkerPool(**kwargs)
        return pool

    return _factory


@pytest.fixture
def process_manager(test_settings):
    """This does trivial creation of the process manager.
    Because it creates multiprocessing Queue objects, we would like to assure shutdown on test teardown.
    """
    pm = None
    try:
        pm = ProcessManager(settings=test_settings)
        yield pm
    finally:
        if pm:
            pm.shutdown()


@pytest.mark.asyncio
async def test_read_results_forever_exits_after_process_manager_shutdown(test_settings, process_manager):
    """Integration-style regression test that closes the real multiprocessing.Queue."""
    loop = asyncio.get_running_loop()
    shared = SharedAsyncObjects()

    instrumented_queue = _InstrumentedQueueWrapper(process_manager.finished_queue, loop)
    process_manager.finished_queue = instrumented_queue
    pool = WorkerPool(process_manager=process_manager, shared=shared, min_workers=0, max_workers=0)
    dispatcher = mock.Mock(spec=DispatcherMain)

    read_task = asyncio.create_task(pool.read_results_forever(dispatcher))

    await instrumented_queue.wait_for_reader()
    process_manager.shutdown()  # should also send sentinal

    await asyncio.wait_for(read_task, timeout=1)


@pytest.mark.asyncio
async def test_read_results_forever_logs_and_skips_messages_missing_keys(caplog):
    class FakeProcessManager:
        def __init__(self, messages):
            self._messages = iter(messages)

        async def read_finished(self, timeout=None):
            return next(self._messages)

        def has_shutdown(self) -> bool:
            return False

    shared = SharedAsyncObjects()
    process_manager = FakeProcessManager(messages=[{"event": "done"}, {"worker": "1"}, "stop"])
    pool = WorkerPool(process_manager=process_manager, shared=shared, min_workers=0, max_workers=0)
    dispatcher = mock.Mock(spec=DispatcherMain)

    caplog.set_level(logging.ERROR, logger="dispatcherd.service.pool")
    await asyncio.wait_for(pool.read_results_forever(dispatcher), timeout=1)

    missing_key_warnings = [record for record in caplog.records if "Results message missing keys" in record.message]
    assert len(missing_key_warnings) == 2


@pytest.mark.asyncio
async def test_scale_to_min(pool_factory):
    "Create 5 workers to fill up to the minimum"
    pool = pool_factory(min_workers=5, max_workers=5)
    assert len(pool.workers) == 0
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers]) == {'initialized'}


@pytest.mark.asyncio
async def test_scale_due_to_queue_pressure(pool_factory):
    "Given 5 busy workers and 1 task in the queue, the scaler should add 1 more worker"
    pool = pool_factory(min_workers=5, max_workers=10)
    await pool.scale_workers()
    for worker in pool.workers:
        worker.status = 'ready'  # a lie, for test
        worker.current_task = {'task': 'waiting.task'}
    pool.queuer.queued_messages = [{'task': 'waiting.task'}]
    assert len(pool.workers) == 5
    await pool.scale_workers()
    assert len(pool.workers) == 6
    assert set([worker.status for worker in pool.workers]) == {'ready', 'initialized'}


@pytest.mark.asyncio
async def test_initialized_workers_count_for_scaling(pool_factory):
    """If we have workers currently scaling up, and queued tasks, we should not scale more workers

    Scaling more workers would not actually get us to the task any faster, and could slow down the system.
    This occurs for the OnStartProducer, that creates tasks which go directly into the queue,
    because the workers have not yet started up.
    With task_ct < worker_ct, we should not scale additional workers right after startup.
    """
    pool = pool_factory(min_workers=5, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers]) == {'initialized'}

    pool.queuer.queued_messages = [{'task': 'waiting.task'} for i in range(5)]  # 5 tasks, 5 workers
    await pool.scale_workers()
    assert len(pool.workers) == 5


@pytest.mark.asyncio
async def test_initialized_and_ready_but_scale(pool_factory):
    """Consider you have 3 OnStart tasks but 2 min workers, you should scale up in this case

    This is a reversal from test_initialized_workers_count_for_scaling,
    as it shows a different case where scaling up beyond min_workers on startup is expected.
    That is, task_ct > worker_ct, on startup.
    """
    pool = pool_factory(min_workers=2, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 2

    pool.queuer.queued_messages = [{'task': 'waiting.task'} for i in range(3)]  # 3 tasks, 2 workers
    await pool.scale_workers()
    assert len(pool.workers) == 3  # grew, added 1 more initialized worker
    assert set([worker.status for worker in pool.workers]) == {'initialized'}  # everything still in startup


@pytest.mark.asyncio
async def test_scale_down_condition(pool_factory):
    """You have 3 workers due to past demand, but work finished long ago. Should scale down."""
    pool = pool_factory(min_workers=1, max_workers=3)

    # Prepare for test by scaling up to the 3 max workers by adding demand
    pool.queuer.queued_messages = [{'task': 'waiting.task'} for i in range(3)]  # 3 tasks, 3 workers
    for i in range(3):
        await pool.scale_workers()
    assert len(pool.workers) == 3
    for worker in pool.workers:
        worker.status = 'ready'  # a lie, for test
        worker.current_task = None
    assert set([worker.status for worker in pool.workers]) == {'ready'}

    # Clear queue and set finished times to long ago
    pool.queuer.queued_messages = []  # queue has been fully worked through, no workers are busy
    pool.last_used_by_ct = {i: time.monotonic() - 120.0 for i in range(30)}  # all work finished 120 seconds ago

    # Outcome of this situation is expected to be a scale-down event
    assert pool.should_scale_down() is True
    await pool.scale_workers()
    # Same number of workers but one worker has been sent a stop signal
    assert len(pool.workers) == 3
    assert set([worker.status for worker in pool.workers]) == {'ready', 'stopping'}


@pytest.mark.asyncio
async def test_scale_up_worker_should_not_be_immediately_eligible_for_scaledown(process_manager):
    """Scaling up due to demand should block a scale-down until the new worker actually does work."""
    shared = SharedAsyncObjects()
    pool = WorkerPool(process_manager=process_manager, shared=shared, min_workers=1, max_workers=5, scaledown_wait=60.0)
    DispatcherMain(producers=(), pool=pool, shared=shared)  # ensure dispatcher objects can be built without mocks

    # Seed pool with ready workers that have been idle long enough to allow scale-down
    existing_workers = 3
    for _ in range(existing_workers):
        worker_id = await pool.up()
        worker = pool.workers.get_by_id(worker_id)
        worker.status = 'ready'
        worker.current_task = None

    idle_timestamp = time.monotonic() - 120.0
    for i in range(existing_workers + 1):
        pool.last_used_by_ct[i] = idle_timestamp  # stale timestamp for a previous high-water mark

    # Queue pressure requires more workers, so scaling up should add one.
    pool.queuer.queued_messages = [{'task': 'waiting.task'} for _ in range(existing_workers + 1)]
    scaled_ct = await pool.scale_workers()
    assert scaled_ct == 1
    assert len([worker for worker in pool.workers if worker.counts_for_capacity]) == existing_workers + 1

    # Because there is still queued work, the new heuristic should block scale-down entirely.
    scaled_ct = await pool.scale_workers()
    assert scaled_ct == 0

    # Add even more demand
    pool.queuer.queued_messages += [{'task': 'waiting.task'}]
    assert pool.active_task_ct() == 5
    assert len(pool.workers) == 4

    # Should still give same result
    scaled_ct = await pool.scale_workers()
    assert scaled_ct == 1
    assert len(pool.workers) == 5  # at max

    # At limit so no more scaling
    pool.queuer.queued_messages += [{'task': 'waiting.task'}]
    assert pool.active_task_ct() == 6
    assert len(pool.workers) == 5
    scaled_ct = await pool.scale_workers()
    assert scaled_ct == 0


@pytest.mark.asyncio
async def test_dispatch_task_holds_management_lock_and_blocks_scaledown(pool_factory):
    """Dispatched work should start while holding the worker lock and block scale-down heuristics."""
    pool = pool_factory(min_workers=1, max_workers=1)
    worker_id = await pool.up()
    worker = pool.workers.get_by_id(worker_id)
    worker.status = 'ready'
    worker.current_task = None

    # Pretend the worker idled long enough that, without new work, scale-down is allowed.
    pool.last_used_by_ct[1] = time.monotonic() - 120.0

    lock_states: list[bool] = []

    async def start_task_under_lock(message):
        lock_states.append(pool.workers.management_lock.locked())
        worker.current_task = message

    worker.start_task = mock.AsyncMock(side_effect=start_task_under_lock)

    message = {'uuid': 'task-123', 'task': 'demo'}
    await pool.dispatch_task(message)

    # Starting the task should have happened while the lock was held and should block scale-down timers.
    assert lock_states == [True]
    assert pool.last_used_by_ct[1] is None
    assert pool.should_scale_down() is False


@pytest.mark.asyncio
async def test_manage_workers_skips_scaledown_when_recently_scaled_up(pool_factory):
    """When scaling up we should avoid the scale-down pass until the new capacity is used."""
    pool = pool_factory()
    pool.scaledown_interval = 0.0

    async def fake_scale_workers():
        pool.shared.exit_event.set()
        return 1  # indicate that we scaled up

    pool.scale_workers = mock.AsyncMock(side_effect=fake_scale_workers)
    pool.manage_new_workers = mock.AsyncMock()
    pool.manage_old_workers = mock.AsyncMock()

    await pool.manage_workers()

    pool.scale_workers.assert_awaited_once()
    pool.manage_new_workers.assert_awaited_once()
    pool.manage_old_workers.assert_not_awaited()


@pytest.mark.asyncio
async def test_error_while_scaling_up(pool_factory):
    """It is always possible that we fail to start workers due to OS errors. This should not error the whole program."""
    pool = pool_factory(min_workers=1, max_workers=1)

    pool.queuer.queued_messages = [{'task': 'waiting.task'}]
    for i in range(3):
        await pool.scale_workers()
    assert len(pool.workers) == 1

    with mock.patch('dispatcherd.service.process.ProcessProxy.start', side_effect=RuntimeError):
        await pool.manage_new_workers()

    assert set([worker.status for worker in pool.workers]) == {'error'}


@pytest.mark.asyncio
async def test_shutdown_is_idepotent(pool_factory):
    """Do some stuff to the pool that is a little weird, but still valid and should not break dispatcherd"""
    pool = pool_factory()
    await pool.shutdown()  # weird to shutdown before starting, but okay
    dispatcher = DispatcherMain(producers=(), pool=pool, shared=pool.shared)
    pool.shared.exit_event.clear()  # pool.shutdown sets this; DispatcherMain.start_working would clear
    await pool.start_working(dispatcher=dispatcher)

    await pool.shutdown()
    await pool.shutdown()  # weird to shutdown twice, but... just return


@pytest.mark.asyncio
async def test_auto_count_max_workers(pool_factory):
    "Test max_workers is set to the number of CPUs if not set"
    cpu_count = multiprocessing.cpu_count()
    pool = pool_factory(max_workers=None)
    assert pool.max_workers == cpu_count
