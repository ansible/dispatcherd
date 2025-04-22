import contextlib
import logging

from typing import Callable, AsyncIterator, Union

import pytest
import pytest_asyncio

from dispatcherd.brokers.pg_notify import Broker, acreate_connection, connection_save
from dispatcherd.config import DispatcherSettings
from dispatcherd.control import Control
from dispatcherd.factories import get_control_from_settings
from dispatcherd.registry import DispatcherMethodRegistry
from dispatcherd.service.main import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service

logger = logging.getLogger(__name__)


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777 application_name=apg_test_server"

BASIC_CONFIG = {
    "version": 2,
    "brokers": {
        "pg_notify": {
            "channels": CHANNELS,
            "config": {'conninfo': CONNECTION_STRING},
            "sync_connection_factory": "dispatcherd.brokers.pg_notify.connection_saver",
            # "async_connection_factory": "dispatcherd.brokers.pg_notify.async_connection_saver",
            "default_publish_channel": "test_channel",
        }
    },
    "producers": {"ControlProducer": {}},
}


@contextlib.asynccontextmanager
async def aconnection_for_test():
    conn = None
    try:
        conn_str = CONNECTION_STRING.replace('application_name=apg_test_server', 'application_name=apg_client')
        conn = await acreate_connection(conninfo=conn_str, autocommit=True)

        # Make sure database is running to avoid deadlocks which can come
        # from using the loop provided by pytest asyncio
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT 1')
            await cursor.fetchall()

        yield conn
    finally:
        if conn:
            await conn.close()


@pytest.fixture(autouse=True)
def clear_connection():
    """Always close connections between tests

    Tests will do a lot of unthoughtful forking, and connections can not
    be shared accross processes.
    """
    yield
    if connection_save._connection:
        connection_save._connection.close()
        connection_save._connection = None
    if connection_save._async_connection:
        connection_save._async_connection.close()
        connection_save._async_connection = None


@pytest.fixture
def conn_config():
    return {'conninfo': CONNECTION_STRING}


@pytest.fixture
def test_settings():
    return DispatcherSettings(BASIC_CONFIG)


@pytest_asyncio.fixture(
    loop_scope="function",
    scope="function",
    params=['ProcessManager', 'ForkServerManager', 'SpawnServerManager'],
    ids=["fork", "forkserver", "spawn"],
)
async def apg_dispatcher(request) -> AsyncIterator[DispatcherMain]:
    this_test_config = BASIC_CONFIG.copy()
    this_test_config.setdefault('service', {})
    this_test_config['service']['process_manager_cls'] = request.param
    async with adispatcher_service(this_test_config) as dispatcher:
        yield dispatcher


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


@pytest.fixture
def get_worker_data():
    "General utility for processing control-with-reply response"
    def _rf(response_list: list[dict[str,Union[str,dict]]]) -> dict:
        "Given some control-and-response data, assuming 1 node, 1 entry, get the task message"
        assert len(response_list) == 1
        response = response_list[0].copy()
        response.pop('node_id', None)
        assert len(response) == 1
        return list(response.values())[0]
    return _rf
