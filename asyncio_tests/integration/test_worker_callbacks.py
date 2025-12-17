from typing import AsyncIterator
import os
import asyncio

import pytest
import pytest_asyncio

from dispatcherd.protocols import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service


LOG_PATH = 'logs/app.log'


@pytest.fixture(scope='session')
def callback_config():
    """No brokers, just the pool with customizations for callback"""
    return {"version": 2, "service": {"main_kwargs": {"node_id": "callback-test-server"}}, "worker": {"worker_cls": "tests.data.callbacks.TestWorker", "worker_kwargs": {"idle_timeout": 0.1}}}


@pytest_asyncio.fixture
async def acallback_dispatcher(callback_config) -> AsyncIterator[DispatcherMain]:
    if os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)

    async with adispatcher_service(callback_config) as dispatcher:
        yield dispatcher


@pytest.mark.asyncio
async def test_worker_callback_usage(acallback_dispatcher):

    await acallback_dispatcher.process_message({
        'task': 'lambda: "This worked!"'
    })

    await asyncio.sleep(0.15)  # to get the idle log

    acallback_dispatcher.shared.exit_event.set()
    await acallback_dispatcher.shutdown()

    with open(LOG_PATH, 'r') as f:
        output = f.read()

    for log in ['on_start', 'on_shutdown', 'pre_task', 'post_task', 'on_idle']:
        assert log in output
