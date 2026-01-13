import queue

import pytest

from dispatcherd.service.process import ProcessManager


@pytest.mark.asyncio
async def test_process_manager_read_finished_timeout_returns_control(test_settings):
    process_manager = ProcessManager(settings=test_settings)
    with pytest.raises(queue.Empty):
        await process_manager.read_finished(timeout=0.05)


@pytest.mark.asyncio
async def test_process_manager_read_finished_without_timeout(test_settings):
    process_manager = ProcessManager(settings=test_settings)

    class RecordingQueue:
        def __init__(self) -> None:
            self.calls = []

        def get(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {'worker': '1', 'event': 'done'}

    recording_queue = RecordingQueue()
    process_manager.finished_queue = recording_queue  # type: ignore[assignment]

    result = await process_manager.read_finished()

    assert result == {'worker': '1', 'event': 'done'}
    assert recording_queue.calls == [((), {})]
