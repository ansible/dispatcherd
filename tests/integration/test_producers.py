import asyncio
import logging

import pytest

from dispatcherd.config import DispatcherSettings
from dispatcherd.factories import from_settings


@pytest.mark.asyncio
async def test_on_start_tasks(caplog):
    dispatcher = None
    try:
        settings = DispatcherSettings(
            {
                'version': 2,
                'service': {'pool_kwargs': {'max_workers': 2}},
                'brokers': {},  # do not need them for this test
                'producers': {'OnStartProducer': {'task_list': {'lambda: "confirmation_of_run"': {}}}},
            }
        )
        dispatcher = from_settings(settings=settings)
        assert dispatcher.pool.finished_count == 0

        await dispatcher.connect_signals()
        with caplog.at_level(logging.DEBUG):
            await dispatcher.start_working()
            await dispatcher.wait_for_producers_ready()
            await asyncio.wait_for(dispatcher.pool.events.work_cleared.wait(), timeout=2)
            await asyncio.sleep(0.02)  # still may be some time between clearing event and desired log

        assert dispatcher.pool.finished_count == 1
        assert 'result: confirmation_of_run' in caplog.text
    finally:
        if dispatcher:
            await dispatcher.shutdown()
            await dispatcher.cancel_tasks()
