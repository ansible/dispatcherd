import multiprocessing
import time

import psycopg
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


VALID_CHANNEL_NAMES = ['foobar', 'foobar🔥', 'foo-bar', '-foo-bar', 'a' * 63]  # just under the limit


BAD_CHANNEL_NAMES = ['a' + '🔥' * 22, 'a' * 64, 'a' * 120, '']  # under 64 but expanded unicode is over  # over the limit of 63


class TestChannelSanitizationPostgresSanity:
    """These do not test dispatcherd itself, but give a reference by testing psycopg and postgres

    These tests validate that the valid and bad channel name lists are, in fact, bad and valid.
    """

    @pytest.mark.parametrize('channel_name', VALID_CHANNEL_NAMES)
    def test_psycopg_valid_sanity_check(self, channel_name, conn_config):
        """Sanity check that postgres itself will accept valid names for listening"""
        conn = psycopg.connect(**conn_config, autocommit=True)
        conn.execute(psycopg.sql.SQL("LISTEN {};").format(psycopg.sql.Identifier(channel_name)))
        conn.execute(Broker.NOTIFY_QUERY_TEMPLATE, (channel_name, 'foo'))

    @pytest.mark.parametrize('channel_name', BAD_CHANNEL_NAMES)
    def test_psycopg_error_sanity_check(self, channel_name, conn_config):
        """Sanity check that postgres itself will raise an error for the known invalid names"""
        conn = psycopg.connect(**conn_config, autocommit=True)
        with pytest.raises(psycopg.DatabaseError):
            conn.execute(psycopg.sql.SQL("LISTEN {};").format(psycopg.sql.Identifier(channel_name)))
            conn.execute(Broker.NOTIFY_QUERY_TEMPLATE, (channel_name, 'foo'))

    @pytest.fixture
    def can_receive_notification(self, conn_config):
        def _rf(channel_name):
            conn = psycopg.connect(**conn_config, autocommit=True)
            try:
                conn.execute(psycopg.sql.SQL("LISTEN {};").format(psycopg.sql.Identifier(channel_name)))
                conn.execute(Broker.NOTIFY_QUERY_TEMPLATE, (channel_name, 'this is a test message'))
            except Exception:
                return False  # did not work
            gen = conn.notifies(timeout=0.001)
            try:
                for notify in gen:
                    assert notify.payload == 'this is a test message'
                    gen.close()
                    return True
                else:
                    return False
            finally:
                gen.close()

        return _rf

    @pytest.mark.parametrize('channel_name', VALID_CHANNEL_NAMES)
    def test_can_receive_over_valid_channels(self, can_receive_notification, channel_name):
        assert can_receive_notification(channel_name)

    @pytest.mark.parametrize('channel_name', BAD_CHANNEL_NAMES)
    def test_can_not_receive_over_invalid_channels(self, can_receive_notification, channel_name):
        assert not can_receive_notification(channel_name)


class TestChannelSanitizationPostgres:
    """These tests verify that we do early validation

    Specifically, this means that dispatcherd will not let you listen to a channel you can not send to
    and that you can not send to a channel you can not listen to"""

    @pytest.mark.parametrize('channel_name', VALID_CHANNEL_NAMES)
    def test_valid_channel_publish(self, channel_name, conn_config):
        broker = Broker(config=conn_config)
        broker.publish_message(channel=channel_name, message='foobar')

    @pytest.mark.parametrize('channel_name', BAD_CHANNEL_NAMES)
    def test_invalid_channel_publish(self, channel_name, conn_config):
        broker = Broker(config=conn_config)
        with pytest.raises(psycopg.DatabaseError):
            broker.publish_message(channel=channel_name, message='foobar')

    @pytest.mark.parametrize('channel_name', VALID_CHANNEL_NAMES)
    def test_valid_channel_listen(self, channel_name, conn_config):
        broker = Broker(config=conn_config, channels=[channel_name])
        broker.process_notify(max_messages=0)

    @pytest.mark.parametrize('channel_name', BAD_CHANNEL_NAMES)
    def test_invalid_channel_listen(self, channel_name, conn_config):
        with pytest.raises(psycopg.DatabaseError):
            broker = Broker(config=conn_config, channels=[channel_name])
            broker.process_notify(max_messages=0)
