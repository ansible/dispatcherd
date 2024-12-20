import asyncio

import pytest

from dispatcher.pool import WorkerPool


@pytest.mark.asyncio
async def test_no_op_task():
    pool = WorkerPool(1)
    await pool.start_working()
    cleared_task = asyncio.create_task(pool.events.queue_cleared.wait())
    await pool.dispatch_task('lambda: None')
    await cleared_task
    await pool.shutdown()
