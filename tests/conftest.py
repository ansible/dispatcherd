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

BASIC_CONFIG = {
    "producers": {
        "BrokeredProducer": {
            # fixture fills in connection details
            "broker": "pg_notify",
            "channels": CHANNELS
        }
    },
    "pool": {
        "max_workers": 3
    }
}


@contextlib.asynccontextmanager
async def aconnection_for_test():
    conn = None
    try:
        conn = await aget_connection({'conninfo': CONNECTION_STRING})

        # Make sure database is running to avoid deadlocks which can come
        # from using the loop provided by pytest asyncio
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT 1')
            await cursor.fetchall()

        yield conn
    finally:
        if conn:
            await conn.close()


@pytest.fixture
def conn_config():
    return {'conninfo': CONNECTION_STRING}


@pytest.fixture
def pg_dispatcher() -> DispatcherMain:
    return DispatcherMain(BASIC_CONFIG)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def apg_dispatcher(conn_config) -> AsyncIterator[DispatcherMain]:
    # need to make a new connection because it can not be same as publisher
    async with aconnection_for_test() as conn:
        config = BASIC_CONFIG.copy()
        config['producers']['BrokeredProducer']['connection'] = conn
        # We have to fill in the config so that replies can still be sent in workers
        # the workers may establish a new psycopg connection
        config['producers']['BrokeredProducer']['config'] = conn_config
        try:
            dispatcher = DispatcherMain(config)

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
