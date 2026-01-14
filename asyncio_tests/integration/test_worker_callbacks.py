import asyncio
import os

import pytest

from dispatcherd.testing.asyncio import adispatcher_service

LOG_PATH = 'logs/app.log'


@pytest.fixture(scope='session')
def callback_config():
    """No brokers, just the pool with customizations for callback"""
    return {
        "version": 2,
        "service": {"main_kwargs": {"node_id": "callback-test-server"}},
        "worker": {"worker_cls": "tests.data.callbacks.TestWorker", "worker_kwargs": {"idle_timeout": 0.1}},
    }


@pytest.mark.asyncio
async def test_worker_callback_usage(callback_config):
    if os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)

    async with adispatcher_service(callback_config) as dispatcher:
        await dispatcher.process_message({'task': 'lambda: "This worked!"'})

        await asyncio.sleep(0.15)  # to get the idle log

        dispatcher.shared.exit_event.set()

    with open(LOG_PATH, 'r') as f:
        output = f.read()

    for log in ['on_start', 'on_shutdown', 'pre_task', 'post_task', 'on_idle']:
        assert log in output
