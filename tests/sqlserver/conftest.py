import os

import pytest
import pytest_asyncio


@pytest.fixture(scope="session")
def config():
    if not os.environ.get("MSSQL_SA_PASSWORD"):
        pytest.skip("Set MSSQL_SA_PASSWORD and start SQL Server for integration tests")
    from database import database_config, provision

    provision()

    class RedactedConfig(dict):
        def __repr__(self):
            return "<SQL Server test configuration; credentials redacted>"

    return RedactedConfig(database_config())


@pytest_asyncio.fixture
async def brokers(config):
    from dispatcherd.brokers.sqlserver import Broker

    objects = []

    def make(channels=(), **kwargs):
        broker = Broker(config, channels=channels, wait_timeout_ms=100, **kwargs)
        objects.append(broker)
        return broker

    yield make
    for broker in objects:
        await broker.aclose()
        broker.close()
