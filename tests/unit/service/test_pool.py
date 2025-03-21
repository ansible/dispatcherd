import asyncio
import time
from unittest import mock

import pytest

from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager


@pytest.mark.asyncio
async def test_scale_to_min(test_settings):
    "Create 5 workers to fill up to the minimum"
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=5)
    assert len(pool.workers) == 0
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers]) == {'initialized'}


@pytest.mark.asyncio
async def test_scale_due_to_queue_pressure(test_settings):
    "Given 5 busy workers and 1 task in the queue, the scaler should add 1 more worker"
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=10)
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
async def test_initialized_workers_count_for_scaling(test_settings):
    """If we have workers currently scaling up, and queued tasks, we should not scale more workers

    Scaling more workers would not actually get us to the task any faster, and could slow down the system.
    This occurs for the OnStartProducer, that creates tasks which go directly into the queue,
    because the workers have not yet started up.
    With task_ct < worker_ct, we should not scale additional workers right after startup.
    """
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers]) == {'initialized'}

    pool.queuer.queued_messages = [{'task': 'waiting.task'} for i in range(5)]  # 5 tasks, 5 workers
    await pool.scale_workers()
    assert len(pool.workers) == 5


@pytest.mark.asyncio
async def test_initialized_and_ready_but_scale(test_settings):
    """Consider you have 3 OnStart tasks but 2 min workers, you should scale up in this case

    This is a reversal from test_initialized_workers_count_for_scaling,
    as it shows a different case where scaling up beyond min_workers on startup is expected.
    That is, task_ct > worker_ct, on startup.
    """
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=2, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 2

    pool.queuer.queued_messages = [{'task': 'waiting.task'} for i in range(3)]  # 3 tasks, 2 workers
    await pool.scale_workers()
    assert len(pool.workers) == 3  # grew, added 1 more initialized worker
    assert set([worker.status for worker in pool.workers]) == {'initialized'}  # everything still in startup


@pytest.mark.asyncio
async def test_scale_down_condition(test_settings):
    """You have 3 workers due to past demand, but work finished long ago. Should scale down."""
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=3)

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
async def test_error_while_scaling_up(test_settings):
    """It is always possible that we fail to start workers due to OS errors. This should not error the whole program."""
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=1)

    pool.queuer.queued_messages = [{'task': 'waiting.task'}]
    for i in range(3):
        await pool.scale_workers()
    assert len(pool.workers) == 1

    with mock.patch('dispatcherd.service.process.ProcessProxy.start', side_effect=RuntimeError):
        await pool.manage_new_workers(asyncio.Lock())

    assert set([worker.status for worker in pool.workers]) == {'error'}
