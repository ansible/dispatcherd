import asyncio
import logging
import multiprocessing
import os
import sys
from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio

from dispatcherd.brokers import get_broker
from dispatcherd.config import DispatcherSettings
from dispatcherd.control import Control
from dispatcherd.factories import get_control_from_settings
from dispatcherd.registry import DispatcherMethodRegistry
from dispatcherd.service.main import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service

logger = logging.getLogger(__name__)


# This will tell the dispatcher_service context manager to collect coverage
os.environ.setdefault('DISPATCHERD_SUBPROCESS_COVERAGE', '1')
os.environ.setdefault('DISPATCHERD_SUBPROCESS_COVERAGE_FILE', '.coverage_subprocess')


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
PG_CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777 application_name=apg_test_server"

# Application tag placed on SQL Server connections created by the dispatcher
# service under test, so disruption tests can find (and kill) its sessions.
SQLSERVER_SERVER_APP = 'dispatcherd_test_server'
SQLSERVER_CLIENT_APP = 'dispatcherd_test_client'


def _resolve_test_backend() -> str:
    """Integration tests default to pg_notify and follow tests/sqlserver's
    MSSQL_SA_PASSWORD convention to pick the sqlserver broker. Set
    DISPATCHERD_TEST_BACKEND to force either backend."""
    forced = os.environ.get('DISPATCHERD_TEST_BACKEND')
    if forced:
        if forced not in ('pg_notify', 'sqlserver'):
            raise RuntimeError(f'Unknown DISPATCHERD_TEST_BACKEND: {forced}')
        return forced
    return 'sqlserver' if os.environ.get('MSSQL_SA_PASSWORD') else 'pg_notify'


BACKEND = _resolve_test_backend()


async def wait_for_processed(pool, expected: int, timeout: float = 3.0) -> None:
    """Wait until the pool has fully processed the expected number of tasks.

    work_cleared alone is not sufficient for slower publish backends, where
    the pool can drain completely between individual publishes.
    """

    async def wait_for_count() -> None:
        while pool.processed_count < expected:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_for_count(), timeout=timeout)


if BACKEND == 'sqlserver':
    # tests/sqlserver is not a package; its helpers are imported the same way
    # its own conftest imports them. In the docker test image the layout is
    # /tests/sqlserver instead of <repo>/tests/sqlserver.
    _sqlserver_dir = Path(__file__).parent / 'tests' / 'sqlserver'
    if not _sqlserver_dir.exists():
        _sqlserver_dir = Path(__file__).parent / 'sqlserver'
    sys.path.insert(0, str(_sqlserver_dir))
    from database import database_config, provision  # type: ignore[import-not-found]


@pytest.fixture(scope='session')
def test_backend():
    """The database backend under test: (broker name, broker kwargs)."""
    if BACKEND == 'pg_notify':
        return 'pg_notify', {'config': {'conninfo': PG_CONNECTION_STRING}}
    provision()
    connection_string = database_config()['connection_string'] + f';APP={SQLSERVER_SERVER_APP}'
    return 'sqlserver', {'config': {'connection_string': connection_string}, 'wait_timeout_ms': 100}


@pytest.fixture(scope='session')
def basic_config(test_backend):
    name, backend_config = test_backend
    entry = {
        'channels': CHANNELS,
        'config': backend_config['config'],
        'default_publish_channel': 'test_channel',
    }
    if name == 'pg_notify':
        entry['sync_connection_factory'] = 'dispatcherd.brokers.pg_notify.connection_saver'
    else:
        entry['wait_timeout_ms'] = backend_config['wait_timeout_ms']
    return {'version': 2, 'brokers': {name: entry}, 'producers': {'ControlProducer': {}}}


@pytest.fixture
def conn_config():
    "PostgreSQL connection info; used by the shared broker contract tests"
    return {'conninfo': PG_CONNECTION_STRING}


@pytest.fixture
def test_settings(basic_config):
    return DispatcherSettings(basic_config)


@pytest_asyncio.fixture(
    loop_scope="function",
    scope="function",
    params=['ProcessManager', 'ForkServerManager', 'SpawnServerManager'],
    ids=["fork", "forkserver", "spawn"],
)
async def db_dispatcher(request, basic_config) -> AsyncIterator[DispatcherMain]:
    this_test_config = basic_config.copy()
    this_test_config.setdefault('service', {})
    this_test_config['service']['process_manager_cls'] = request.param
    ctx = multiprocessing.get_context()
    lock = ctx.Lock()
    lock.acquire()
    lock.release()
    async with adispatcher_service(this_test_config) as dispatcher:
        yield dispatcher


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def db_control(test_settings) -> AsyncIterator[Control]:
    return get_control_from_settings(settings=test_settings)


@pytest_asyncio.fixture(loop_scope="function", scope="function")
async def db_message(test_backend):
    "Publish messages through a backend-owned publisher connection"
    name, backend_config = test_backend
    if name == 'sqlserver':
        config = {
            'config': {
                'connection_string': backend_config['config']['connection_string'].replace(f'APP={SQLSERVER_SERVER_APP}', f'APP={SQLSERVER_CLIENT_APP}')
            },
            'wait_timeout_ms': backend_config['wait_timeout_ms'],
        }
    else:
        conninfo = backend_config['config']['conninfo'].replace('application_name=apg_test_server', 'application_name=apg_client')
        config = {'config': {'conninfo': conninfo}}
    publisher = get_broker(name, config, default_publish_channel='test_channel')

    async def _rf(message, channel=None):
        await publisher.apublish_message(channel=channel, message=message)

    yield _rf
    await publisher.aclose()
    publisher.close()


@pytest.fixture
def registry() -> DispatcherMethodRegistry:
    "Return a fresh registry, separate from the global one, for testing"
    return DispatcherMethodRegistry()


@pytest.fixture
def get_worker_data():
    "General utility for processing control-with-reply response"

    def _rf(response_list: list[dict[str, str | dict]]) -> dict:
        "Given some control-and-response data, assuming 1 node, 1 entry, get the task message"
        assert len(response_list) == 1
        response = response_list[0].copy()
        response.pop('node_id', None)
        assert len(response) == 1
        return list(response.values())[0]

    return _rf
