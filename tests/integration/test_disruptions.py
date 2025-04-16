import asyncio

import pytest

import psycopg

from tests.conftest import CONNECTION_STRING

from dispatcherd.producers.brokered import BrokeredProducer


# Change the application_name so that when we run this test we will not kill the connection for the test itself
THIS_TEST_STR = CONNECTION_STRING.replace('application_name=apg_test_server', 'application_name=do_not_delete_me')


@pytest.mark.asyncio
async def test_sever_pg_connection(apg_dispatcher, pg_message):
    brokered_producers = [producer for producer in apg_dispatcher.producers if isinstance(producer, BrokeredProducer)]
    assert len(brokered_producers) == 1
    brokered_producers[0].events.ready_event.clear()

    query = """
    SELECT pid, usename, application_name, backend_start, state
    FROM pg_stat_activity
    WHERE state IS NOT NULL
    AND application_name = 'apg_test_server'
    ORDER BY backend_start DESC;
    """
    # Asynchronously connect to PostgreSQL using a connection string
    async with await psycopg.AsyncConnection.connect(THIS_TEST_STR) as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            connections = await cur.fetchall()

            pids = []
            print(f'Found following connections, will kill connections for those pids')
            for row in connections:
                pids.append(row[0])
                print('pid, user, app_name, backend_start, state')
                print(row)

            assert len(pids) == 1

            for pid in pids:
                await cur.execute(f"SELECT pg_terminate_backend({pid});")

    # Main loop (which test simulates) should wake up before infinity
    await asyncio.wait_for(apg_dispatcher.main_loop_wait(), timeout=3)
    assert not apg_dispatcher.shared.exit_event.is_set()
    # Okay now fix the things
    await apg_dispatcher.recycle_broker_producers()

    # Continue method after the producers are ready, an effect of the recycle
    ready_event_task = asyncio.create_task(brokered_producers[0].events.ready_event.wait(), name='test_ready_event')
    await asyncio.wait_for(ready_event_task, timeout=5)

    # Submitting a new task should now work
    assert apg_dispatcher.pool.finished_count == 0
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait(), name='test_lambda_clear_wait')
    await pg_message('lambda: "This worked!"')
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == 1
