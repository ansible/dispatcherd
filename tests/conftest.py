import asyncio

import contextlib

from typing import Callable, AsyncIterator

import pytest

import pytest_asyncio

from dispatcher.main import DispatcherMain
from dispatcher.control import Control

from dispatcher.brokers.pg_notify import apublish_message, aget_connection, get_connection


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"

BASIC_CONFIG = {"producers": {"brokers": {"pg_notify": {"conninfo": CONNECTION_STRING}, "channels": CHANNELS}}, "pool": {"max_workers": 3}}


@pytest.fixture
def pg_dispatcher() -> DispatcherMain:
    return DispatcherMain(BASIC_CONFIG)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def apg_dispatcher(request) -> AsyncIterator[DispatcherMain]:
    try:
        dispatcher = DispatcherMain(BASIC_CONFIG)

        await dispatcher.connect_signals()
        await dispatcher.start_working()
        await dispatcher.wait_for_producers_ready()

        yield dispatcher
    finally:
        await dispatcher.shutdown()
        await dispatcher.cancel_tasks()


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def pg_message(psycopg_conn) -> Callable:
    async def _rf(message, channel='test_channel'):
        await apublish_message(psycopg_conn, channel, message)
    return _rf


@pytest.fixture
def conn_config():
    return {'conninfo': CONNECTION_STRING}


@contextlib.asynccontextmanager
async def aconnection_for_test():
    conn = None
    try:
        conn = await aget_connection({'conninfo': CONNECTION_STRING})
        yield conn
    finally:
        if conn:
            await conn.close()


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def pg_control() -> AsyncIterator[Control]:
    """This has to use a different connection from dispatcher itself

    because psycopg will pool async connections, meaning that submission
    for the control task would be blocked by the listening query of the dispatcher itself"""
    async with aconnection_for_test() as conn:
        yield Control('test_channel', async_connection=conn)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def psycopg_conn():
    async with aconnection_for_test() as conn:
        yield conn
