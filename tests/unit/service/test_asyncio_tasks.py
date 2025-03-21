import asyncio

import pytest

from dispatcherd.service.asyncio_tasks import ensure_fatal


async def will_fail():
    raise RuntimeError()


@pytest.mark.asyncio
async def test_capture_initial_task_failure():
    event = asyncio.Event()
    assert not event.is_set()
    aio_task = asyncio.create_task(will_fail())
    with pytest.raises(RuntimeError):
        ensure_fatal(aio_task, exit_event=event)
        await aio_task
    assert event.is_set()


async def will_fail_soon():
    await asyncio.sleep(0.01)
    raise RuntimeError()


@pytest.mark.asyncio
async def test_capture_later_task_failure():
    event = asyncio.Event()
    assert not event.is_set()
    aio_task = asyncio.create_task(will_fail_soon())
    with pytest.raises(RuntimeError):
        ensure_fatal(aio_task, exit_event=event)
        await aio_task
    assert event.is_set()


async def good_task():
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_task_does_not_fail_so_okay():
    event = asyncio.Event()
    assert not event.is_set()
    aio_task = asyncio.create_task(good_task())
    ensure_fatal(aio_task, exit_event=event)
    await aio_task
    assert not event.is_set()
