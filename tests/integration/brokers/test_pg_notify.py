import pytest

from dispatcher.brokers.pg_notify import Broker, create_connection, acreate_connection


def test_sync_connection_from_config_reuse(conn_config):
    broker = Broker(config=conn_config)
    conn = broker.get_connection()
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        assert cur.fetchall() == [(1,)]

    conn2 = broker.get_connection()
    assert conn is conn2

    assert conn is not create_connection(**conn_config)


@pytest.mark.asyncio
async def test_async_connection_from_config_reuse(conn_config):
    broker = Broker(config=conn_config)
    conn = await broker.aget_connection()
    async with conn.cursor() as cur:
        await cur.execute('SELECT 1')
        assert await cur.fetchall() == [(1,)]

    conn2 = await broker.aget_connection()
    assert conn is conn2

    assert conn is not await acreate_connection(**conn_config)
