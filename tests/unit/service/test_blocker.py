import pytest

from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.registry import registry

from tests.data.methods import print_hello


@pytest.mark.asyncio
async def test_block_multiple_tasks(test_settings):
    dmethod = registry.get_from_callable(print_hello)
    task_data = dmethod.get_async_body(on_duplicate='serial')
    assert task_data['on_duplicate'] == 'serial'

    pm = ProcessManager(settings=test_settings)
    pool = WorkerPool(pm, min_workers=5, max_workers=5, shared=SharedAsyncObjects())

    await pool.dispatch_task(task_data.copy())
    assert list(pool.queuer) == [task_data]
    assert list(pool.queuer.active_tasks()) == [task_data]

    for i in range(2):
        await pool.dispatch_task(task_data.copy())

    # both new tasks will queue
    assert len(list(pool.blocker)) == 2

    # work finished
    pool.queuer.queued_messages = []

    await pool.drain_queue()

    # One task advanced from the blocker to the queuer
    assert len(list(pool.blocker)) == 1
    assert len(list(pool.queuer)) == 1
