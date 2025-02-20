import contextlib

from typing import Callable, AsyncIterator

import pytest

import pytest_asyncio

from dispatcher.main import DispatcherMain
from dispatcher.control import Control

from dispatcher.brokers.pg_notify import Broker, create_connection, acreate_connection
from dispatcher.registry import DispatcherMethodRegistry
from dispatcher.config import DispatcherSettings
from dispatcher.factories import from_settings, get_control_from_settings


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"

BASIC_CONFIG = {
    "version": 2,
    "brokers": {
        "pg_notify": {
            "channels": CHANNELS,
            "config": {'conninfo': CONNECTION_STRING},
            "sync_connection_factory": "dispatcher.brokers.pg_notify.connection_saver",
            # "async_connection_factory": "dispatcher.brokers.pg_notify.async_connection_saver",
            "default_publish_channel": "test_channel"
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
        conn = await acreate_connection(conninfo=CONNECTION_STRING, autocommit=True)

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
    # We can not reuse the connection between tests
    config = BASIC_CONFIG.copy()
    config['brokers']['pg_notify'].pop('async_connection_factory')
    return DispatcherMain(config)


@pytest.fixture
def test_settings():
    return DispatcherSettings(BASIC_CONFIG)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def apg_dispatcher(test_settings) -> AsyncIterator[DispatcherMain]:
    dispatcher = None
    try:
        dispatcher = from_settings(settings=test_settings)

        await dispatcher.connect_signals()
        await dispatcher.start_working()
        await dispatcher.wait_for_producers_ready()

        assert dispatcher.pool.finished_count == 0  # sanity

        yield dispatcher
    finally:
        if dispatcher:
            await dispatcher.shutdown()
            await dispatcher.cancel_tasks()


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def pg_control(test_settings) -> AsyncIterator[Control]:
    return get_control_from_settings(settings=test_settings)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def psycopg_conn():
    async with aconnection_for_test() as conn:
        yield conn


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def pg_message(psycopg_conn) -> Callable:
    async def _rf(message, channel=None):
        # Note on weirdness here, this broker will only be used for async publishing, so we give junk for synchronous connection
        broker = Broker(async_connection=psycopg_conn, default_publish_channel='test_channel', sync_connection_factory='tests.data.methods.something')
        await broker.apublish_message(channel=channel, message=message)
    return _rf


@pytest.fixture
def registry() -> DispatcherMethodRegistry:
    "Return a fresh registry, separate from the global one, for testing"
    return DispatcherMethodRegistry()
