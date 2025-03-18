import asyncio
import logging
import os
from typing import AsyncIterator, Callable

import pytest
import pytest_asyncio

from dispatcherd.config import DispatcherSettings
from dispatcherd.control import Control
from dispatcherd.factories import get_control_from_settings, get_publisher_from_settings
from dispatcherd.protocols import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def sock_path(tmp_path_factory):
    return str(tmp_path_factory.mktemp("socket") / 'test.sock')


@pytest.fixture(scope='session')
def socket_config(sock_path):
    return {"version": 2, "brokers": {"socket": {"socket_path": sock_path}}, "service": {"main_kwargs": {"node_id": "socket-test-server"}}}


@pytest.fixture(scope='session')
def socket_settings(socket_config):
    return DispatcherSettings(socket_config)


@pytest_asyncio.fixture
async def asock_dispatcher(socket_config) -> AsyncIterator[DispatcherMain]:
    async with adispatcher_service(socket_config) as dispatcher:
        yield dispatcher


@pytest_asyncio.fixture
async def sock_control(socket_settings) -> AsyncIterator[Control]:
    return get_control_from_settings(settings=socket_settings)


@pytest_asyncio.fixture
async def sock_broker(socket_settings) -> Callable:
    broker = get_publisher_from_settings(settings=socket_settings)
    assert not broker.clients  # make sure this is new for client, not the server
    return broker


@pytest.mark.asyncio
async def test_run_lambda_function_socket(asock_dispatcher, sock_broker):
    starting_ct = asock_dispatcher.pool.finished_count
    clearing_task = asyncio.create_task(asock_dispatcher.pool.events.work_cleared.wait(), name='test_lambda_clear_wait')

    assert sock_broker.sock is None  # again, confirm this is a distinct client broker
    await sock_broker.apublish_message(message='lambda: "This worked!"')

    await asyncio.wait_for(clearing_task, timeout=1)

    assert asock_dispatcher.pool.finished_count == starting_ct + 1


@pytest.mark.asyncio
async def test_run_lambda_function_socket_sync_client(asock_dispatcher, sock_broker):
    starting_ct = asock_dispatcher.pool.finished_count
    clearing_task = asyncio.create_task(asock_dispatcher.pool.events.work_cleared.wait(), name='test_lambda_clear_wait')

    sock_broker.publish_message(message='lambda: "This worked!"')

    await asyncio.wait_for(clearing_task, timeout=1)

    assert asock_dispatcher.pool.finished_count == starting_ct + 1


@pytest.mark.asyncio
async def test_simple_control_and_reply(asock_dispatcher, sock_control):
    loop = asyncio.get_event_loop()

    def alive_cmd():
        return sock_control.control_with_reply('alive')

    alive = await loop.run_in_executor(None, alive_cmd)
    assert len(alive) == 1
    data = alive[0]

    assert data['node_id'] == 'socket-test-server'


@pytest.mark.asyncio
async def test_socket_tasks_are_named(asock_dispatcher, sock_control, python312):
    loop = asyncio.get_event_loop()

    def aio_tasks_cmd():
        return sock_control.control_with_reply('aio_tasks')

    aio_tasks = await loop.run_in_executor(None, aio_tasks_cmd)

    current_task_name = asyncio.current_task().get_name()

    assert len(aio_tasks) == 1
    data = aio_tasks[0]
    for task_name, task_stuff in data.items():
        if task_name == current_task_name:
            continue
        assert not task_name.startswith('Task-'), task_stuff['stack']
