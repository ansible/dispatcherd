import asyncio
import time

import pytest

from dispatcher.service.asyncio_tasks import ensure_fatal


async def will_fail():
    raise RuntimeError()


@pytest.mark.asyncio
async def test_capture_initial_task_failure():
    aio_task = asyncio.create_task(will_fail())
    with pytest.raises(RuntimeError):
        ensure_fatal(aio_task)
        await aio_task


async def will_fail_soon():
    await asyncio.sleep(0.01)
    raise RuntimeError()


@pytest.mark.asyncio
async def test_capture_later_task_failure():
    aio_task = asyncio.create_task(will_fail_soon())
    with pytest.raises(RuntimeError):
        ensure_fatal(aio_task)
        await aio_task


async def good_task():
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_task_does_not_fail_so_okay():
    aio_task = asyncio.create_task(good_task())
    ensure_fatal(aio_task)
    await aio_task
