import asyncio
import time
from unittest import mock

import pytest

from dispatcherd.service.next_wakeup_runner import HasWakeup, NextWakeupRunner
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects


class ObjectWithWakeup(HasWakeup):
    def __init__(self, period):
        self.period = period
        self.last_run = time.monotonic()

    def next_wakeup(self):
        if self.period is None:
            return None
        return self.last_run + self.period


@pytest.mark.asyncio
async def test_process_wakeups(current_time=time.monotonic(), do_processing=False):
    objects = set()
    obj = ObjectWithWakeup(1)
    objects.add(obj)
    callback = mock.MagicMock()
    runner = NextWakeupRunner(objects, callback, shared=SharedAsyncObjects())
    assert await runner.process_wakeups(current_time=time.monotonic(), do_processing=False) > time.monotonic()
    assert await runner.process_wakeups(current_time=time.monotonic(), do_processing=False) < time.monotonic() + 1.0

    obj.last_run = time.monotonic() + 0.1
    assert await runner.process_wakeups(current_time=time.monotonic(), do_processing=False) > time.monotonic() + 1.0

    obj.period = None
    assert await runner.process_wakeups(current_time=time.monotonic(), do_processing=False) is None

    callback.assert_not_called()


@pytest.mark.asyncio
async def test_run_and_exit_task():
    objects = set()
    obj = ObjectWithWakeup(0.01)  # runs in 0.01 seconds, test will take this long
    objects.add(obj)

    async def callback_makes_done(t):
        obj.period = None  # No need to run ever again
        callback_makes_done.is_called = True

    runner = NextWakeupRunner(objects, callback_makes_done, shared=SharedAsyncObjects())

    await runner.background_task()  # should finish

    assert callback_makes_done.is_called is True


@pytest.mark.asyncio
async def test_next_wakeup_is_in_past():
    objects = set()
    obj = ObjectWithWakeup(1)  # runs every second
    obj.last_run -= 11.0  # next run is now a while ago
    objects.add(obj)

    async def normal_periodic_callback(t):
        obj.last_run = time.monotonic()  # back to normal schedule
        normal_periodic_callback.is_called = True

    normal_periodic_callback.is_called = False

    runner = NextWakeupRunner(objects, normal_periodic_callback, shared=SharedAsyncObjects())

    # run the background task asynchronously
    background_task = asyncio.create_task(runner.background_task())

    # Now we will poll for our desired result
    for _ in range(10):
        await asyncio.sleep(0.01)  # this should be immediate we just do not want race conditions
        if normal_periodic_callback.is_called:
            break
    else:
        raise RuntimeError('never called the callback')

    # now that our test is done we can shut down the background task runner
    runner.shared.exit_event.set()
    await runner.kick()

    # the task should reach its exit condition for shutdown now
    await background_task


@pytest.mark.asyncio
async def test_graceful_shutdown():
    objects = set()
    obj = ObjectWithWakeup(1)
    obj.last_run -= 1.0  # make first run immediate
    objects.add(obj)
    callback = mock.MagicMock()

    async def mock_process_tasks(t):
        obj.last_run = time.monotonic()  # did whatever we do with the things
        callback()  # track for assertion

    runner = NextWakeupRunner(objects, mock_process_tasks, shared=SharedAsyncObjects())
    await runner.kick()  # creates task, starts running

    # Poll for the objects data to reflect that it has been processed
    for _ in range(10):
        await asyncio.sleep(0.01)
        if obj.last_run >= time.monotonic() - 1.0:
            break
    else:
        raise RuntimeError('Object was never marked as ran as expected')

    runner.shared.exit_event.set()
    await runner.kick()
    await runner.asyncio_task
    assert runner.asyncio_task.done()

    callback.assert_called_once_with()

    assert runner.asyncio_task.done() is True
