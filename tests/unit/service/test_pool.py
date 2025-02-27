import time

import pytest

from dispatcher.service.pool import WorkerPool
from dispatcher.service.process import ProcessManager


@pytest.mark.asyncio
async def test_scale_to_min(test_settings):
    "Create 5 workers to fill up to the minimum"
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=5)
    assert len(pool.workers) == 0
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers.values()]) == {'initialized'}


@pytest.mark.asyncio
async def test_scale_due_to_queue_pressure(test_settings):
    "Given 5 busy workers and 1 task in the queue, the scaler should add 1 more worker"
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=10)
    await pool.scale_workers()
    for worker in pool.workers.values():
        worker.status = 'ready'  # a lie, for test
        worker.current_task = {'task': 'waiting.task'}
    pool.queued_messages = [{'task': 'waiting.task'}]
    assert len(pool.workers) == 5
    await pool.scale_workers()
    assert len(pool.workers) == 6
    assert set([worker.status for worker in pool.workers.values()]) == {'ready', 'initialized'}


@pytest.mark.asyncio
async def test_initialized_workers_count_for_scaling(test_settings):
    """If we have workers currently scaling up, and queued tasks, we should not scale more workers

    Scaling more workers would not actually get us to the task any faster, and could slow down the system.
    This occurs for the OnStartProducer, that creates tasks which go directly into the queue,
    because the workers have not yet started up.
    With tasks < worker_ct, we should not scale additional workers right after startup.
    """
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 5
    assert set([worker.status for worker in pool.workers.values()]) == {'initialized'}

    pool.queued_messages = [{'task': 'waiting.task'} for i in range(5)]  # 5 tasks, 5 workers
    await pool.scale_workers()
    assert len(pool.workers) == 5


@pytest.mark.asyncio
async def test_initialized_and_ready_but_scale(test_settings):
    """Consider you have 3 OnStart tasks but 2 min workers, you should scale up in this case"""
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=2, max_workers=10)
    await pool.scale_workers()
    assert len(pool.workers) == 2

    pool.queued_messages = [{'task': 'waiting.task'} for i in range(3)]  # 3 tasks, 2 workers
    await pool.scale_workers()
    assert len(pool.workers) == 3  # grew, added 1 more initialized worker
    assert set([worker.status for worker in pool.workers.values()]) == {'initialized'}


@pytest.mark.asyncio
async def test_scale_down_condition(test_settings):
    """Consider you have 3 OnStart tasks but 2 min workers, you should scale up in this case"""
    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=1, max_workers=3)
    pool.queued_messages = [{'task': 'waiting.task'} for i in range(3)]  # 3 tasks, 3 workers
    for i in range(3):
        await pool.scale_workers()
    assert len(pool.workers) == 3
    for worker in pool.workers.values():
        worker.status = 'ready'  # a lie, for test
        worker.current_task = None
    assert set([worker.status for worker in pool.workers.values()]) == {'ready'}

    pool.queued_messages = []  # queue has been fully worked through, no workers are busy
    pool.last_used_by_ct = {i: time.monotonic() - 120. for i in range(30)}  # all work finished 120 seconds ago
    assert pool.should_scale_down() is True
    await pool.scale_workers()
    # Same number of workers but one worker has been sent a stop signal
    assert len(pool.workers) == 3
    assert set([worker.status for worker in pool.workers.values()]) == {'ready', 'stopping'}
