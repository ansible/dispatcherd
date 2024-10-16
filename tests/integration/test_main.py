import asyncio

import pytest

from dispatcher.brokers.pg_notify import publish_message


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel2']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


@pytest.mark.asyncio
async def test_run_and_then_shutdown(pg_dispatcher):
    await pg_dispatcher.start_working()
    await asyncio.sleep(2)

    await pg_dispatcher.shutdown()

    assert pg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_run_lambda_function(pg_dispatcher):
    await pg_dispatcher.start_working()
    await asyncio.sleep(1)

    # TODO: do config better
    publish_message('test_channel', 'lambda: "This worked!"', config={"conninfo": CONNECTION_STRING})
    await asyncio.sleep(1)

    await pg_dispatcher.shutdown()

    assert pg_dispatcher.pool.finished_count == 1
