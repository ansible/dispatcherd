import multiprocessing
import time

import pytest

from dispatcherd.brokers.pg_notify import Broker, acreate_connection, create_connection


def test_sync_connection_from_config_reuse(conn_config):
    broker = Broker(config=conn_config)
    conn = broker.get_connection()
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        assert cur.fetchall() == [(1,)]

    conn2 = broker.get_connection()
    assert conn is conn2

    assert conn is not create_connection(**conn_config)


def test_sync_listen_timeout(conn_config):
    broker = Broker(config=conn_config)
    timeout_value = 0.05
    start = time.monotonic()
    assert list(broker.process_notify(timeout=timeout_value)) == []
    delta = time.monotonic() - start
    assert delta > timeout_value


def _send_message(conn_config):
    broker = Broker(config=conn_config)
    if broker._sync_connection:
        broker._sync_connection.close()

    broker.publish_message('test_sync_listen_receive', 'test_message')


def test_sync_listen_receive(conn_config):
    messages = []
    with multiprocessing.Pool(processes=1) as pool:

        def send_from_subprocess():
            pool.apply(_send_message, args=(conn_config,))

        broker = Broker(config=conn_config, channels=('test_sync_listen_receive',))
        timeout_value = 2.0
        start = time.monotonic()
        for channel, message in broker.process_notify(connected_callback=send_from_subprocess, timeout=timeout_value):
            messages.append(message)
        delta = time.monotonic() - start

    assert messages == ['test_message']
    assert delta < timeout_value


def test_sync_listen_receive_multi_message(conn_config):
    """Tests that the expected messages exit condition works, we get 3 messages, not just 1"""
    messages = []
    with multiprocessing.Pool(processes=1) as pool:

        def send_from_subprocess():
            pool.apply(_send_message, args=(conn_config,))
            pool.apply(_send_message, args=(conn_config,))
            pool.apply(_send_message, args=(conn_config,))

        broker = Broker(config=conn_config, channels=('test_sync_listen_receive',))
        timeout_value = 2.0
        start = time.monotonic()
        for channel, message in broker.process_notify(connected_callback=send_from_subprocess, max_messages=3):
            messages.append(message)
        delta = time.monotonic() - start

    assert messages == ['test_message' for i in range(3)]
    assert delta < timeout_value


def test_get_message_then_timeout(conn_config):
    """Tests that the expected messages exit condition works, we get 3 messages, not just 1"""
    messages = []
    with multiprocessing.Pool(processes=1) as pool:

        def send_from_subprocess():
            pool.apply(_send_message, args=(conn_config,))

        broker = Broker(config=conn_config, channels=('test_sync_listen_receive',))
        timeout_value = 0.5
        start = time.monotonic()
        for channel, message in broker.process_notify(connected_callback=send_from_subprocess, timeout=timeout_value, max_messages=2):
            messages.append(message)
        delta = time.monotonic() - start

    assert messages == ['test_message']
    assert delta > timeout_value  # goes until timeout


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
