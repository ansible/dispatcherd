import asyncio
import json
from types import SimpleNamespace

import pytest

from dispatcherd.chunking import split_message
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.main import DispatcherMain


class DummyPool:
    def __init__(self, shared):
        self.shared = shared
        self.events = SimpleNamespace(
            workers_ready=asyncio.Event(),
            queue_cleared=asyncio.Event(),
            work_cleared=asyncio.Event(),
            management_event=asyncio.Event(),
        )
        self.dispatched_messages = []

    async def start_working(self, dispatcher):
        return

    async def dispatch_task(self, message):
        self.dispatched_messages.append(message)

    async def shutdown(self):
        return

    def get_status_data(self):
        return {}


@pytest.mark.asyncio
async def test_dispatcher_drops_stale_chunked_messages():
    shared = SharedAsyncObjects()
    pool = DummyPool(shared)
    dispatcher = DispatcherMain(
        producers=(),
        pool=pool,
        shared=shared,
        chunk_message_timeout_seconds=0.05,
    )

    await dispatcher.start_working()

    base_message = json.dumps({'task': 'lambda: "ok"', 'uuid': 'complete', 'data': 'x' * 500})
    base_chunks = [json.loads(chunk) for chunk in split_message(base_message, max_bytes=256)]

    for chunk in base_chunks:
        await dispatcher.process_message(chunk)
    assert len(pool.dispatched_messages) == 1  # message assembled successfully

    stale_message = json.dumps({'task': 'lambda: "late"', 'uuid': 'stale', 'data': 'z' * 500})
    stale_chunks = [json.loads(chunk) for chunk in split_message(stale_message, max_bytes=256)]

    await dispatcher.process_message(stale_chunks[0])
    await asyncio.sleep(0.1)
    for chunk in stale_chunks[1:]:
        await dispatcher.process_message(chunk)

    # Final chunk alone should not dispatch because the accumulator dropped stale fragments.
    assert len(pool.dispatched_messages) == 1

    await dispatcher.shutdown()
