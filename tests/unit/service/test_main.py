import asyncio
from types import SimpleNamespace

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.main import DispatcherMain
from dispatcherd.testing import wait_for_producers_ready


class DummyPool:
    def __init__(self) -> None:
        self.events = SimpleNamespace(workers_ready=asyncio.Event())
        self.finished_count = 0

    async def start_working(self, dispatcher: DispatcherMain) -> None:  # pragma: no cover - not used in these tests
        return

    async def dispatch_task(self, message: dict) -> None:  # pragma: no cover - not used in these tests
        return

    async def shutdown(self) -> None:
        return


class HangingProducer:
    def __init__(self) -> None:
        self.events = SimpleNamespace(ready_event=asyncio.Event(), recycle_event=asyncio.Event())
        self.can_recycle = False

    async def start_producing(self, dispatcher: DispatcherMain) -> None:  # pragma: no cover - not used
        return

    def get_status_data(self) -> dict:
        return {}

    async def shutdown(self) -> None:
        self.events.ready_event.set()

    def all_tasks(self) -> list[asyncio.Task]:
        return []

    async def recycle(self) -> None:  # pragma: no cover - not used
        return


def _dispatcher_with_hanging_producer() -> DispatcherMain:
    shared = SharedAsyncObjects()
    pool = DummyPool()
    producer = HangingProducer()
    dispatcher = DispatcherMain(producers=[producer], pool=pool, shared=shared)
    return dispatcher


def _pending_tmp_wait_tasks() -> list[asyncio.Task]:
    return [task for task in asyncio.all_tasks() if 'tmp_' in task.get_name() and not task.done()]


@pytest.mark.asyncio
async def test_wait_for_producers_ready_exits_on_force_shutdown():
    dispatcher = _dispatcher_with_hanging_producer()
    wait_task = asyncio.create_task(wait_for_producers_ready(dispatcher))
    await asyncio.sleep(0)

    dispatcher.shared.exit_event.set()

    await asyncio.wait_for(wait_task, timeout=0.1)
    assert wait_task.done()
    assert not _pending_tmp_wait_tasks()

    await dispatcher.shutdown()


@pytest.mark.asyncio
async def test_wait_for_producers_ready_cleanup_on_cancel():
    dispatcher = _dispatcher_with_hanging_producer()
    wait_task = asyncio.create_task(wait_for_producers_ready(dispatcher))

    await asyncio.sleep(0)
    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert not _pending_tmp_wait_tasks()

    await dispatcher.shutdown()


@pytest.mark.asyncio
async def test_wait_for_producers_ready_cleanup_on_timeout():
    dispatcher = _dispatcher_with_hanging_producer()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(wait_for_producers_ready(dispatcher), timeout=0.01)

    assert not _pending_tmp_wait_tasks()

    await dispatcher.shutdown()
