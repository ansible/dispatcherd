import asyncio
import logging

import pytest

from dispatcherd.service.asyncio_tasks import cancel_other_tasks, ensure_fatal


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


@pytest.mark.asyncio
async def test_task_cancelled_does_not_trigger_exit():
    event = asyncio.Event()

    async def sleepy_task():
        await asyncio.sleep(0.1)

    aio_task = asyncio.create_task(sleepy_task())
    ensure_fatal(aio_task, exit_event=event)
    aio_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await aio_task
    assert not event.is_set()


@pytest.mark.asyncio
async def test_finished_task_failure_sets_exit_event():
    event = asyncio.Event()
    aio_task = asyncio.create_task(will_fail())
    # Allow the task to finish and fail before we attach ensure_fatal.
    await asyncio.sleep(0)

    assert aio_task.done()
    with pytest.raises(RuntimeError):
        ensure_fatal(aio_task, exit_event=event)
    assert event.is_set()


@pytest.mark.asyncio
async def test_cancel_other_tasks_logs_exceptions(caplog):
    caplog.set_level(logging.ERROR)

    async def exploding_task():
        try:
            while True:
                await asyncio.sleep(0.01)
        finally:
            raise RuntimeError('boom')

    worker = asyncio.create_task(exploding_task(), name='exploding_task')
    await asyncio.sleep(0)

    await cancel_other_tasks()

    assert worker.done()
    assert any('raised while awaiting shutdown completion' in record.getMessage() and 'exploding_task' in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
async def test_cancel_other_tasks_reports_pending_tasks(caplog):
    caplog.set_level(logging.WARNING)
    release = asyncio.Event()

    async def stubborn_task():
        try:
            while True:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            await release.wait()
            raise

    worker = asyncio.create_task(stubborn_task(), name='stubborn_task')
    await asyncio.sleep(0)

    await cancel_other_tasks(timeout=0.0)

    assert any('Timed out waiting 0.0s for task stubborn_task to finish' in record.getMessage() for record in caplog.records)

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await worker
