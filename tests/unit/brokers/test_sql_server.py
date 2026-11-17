import asyncio
import threading

import pytest

from dispatcherd.brokers.sql_server import Broker, _build_connection_string


class FakeCursor:
    def __init__(self, rows=()):
        self.calls = []
        self.rows = list(rows)
        self.execute_thread_ids = []
        self.execute_error = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def execute(self, query, params=()):
        self.execute_thread_ids.append(threading.get_ident())
        if self.execute_error is not None:
            raise self.execute_error
        self.calls.append((query, params))
        return self

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows=()):
        self.cursor_object = FakeCursor(rows)
        self.closed = False
        self.timeout = None

    def cursor(self):
        return self.cursor_object

    def close(self):
        self.closed = True


def test_connection_string_escapes_delimiters_in_credentials():
    connection_string = _build_connection_string(
        {
            "server": "localhost",
            "port": 1433,
            "database": "dispatcherd",
            "user": "sa",
            "password": "semi;brace}",
        }
    )

    assert "SERVER=localhost,1433;" in connection_string
    assert "PWD={semi;brace}}};" in connection_string
    assert "DRIVER={ODBC Driver 18 for SQL Server};" in connection_string


def test_publish_inserts_channel_and_full_payload_as_parameters(caplog):
    connection = FakeConnection()
    broker = Broker(config={}, channels=("metrics",), sync_connection=connection)
    payload = '{"task":"metrics.collect","kwargs":{"value":"snowman ☃"}}'

    channel = broker.publish_message(channel="metrics", message=payload)

    assert channel == "metrics"
    assert connection.cursor_object.calls == [("INSERT INTO dbo.dispatcherd_messages (channel, payload) VALUES (?, ?)", ("metrics", payload))]
    assert "snowman ☃" not in caplog.text


def test_connection_query_timeout_is_configurable():
    connection = FakeConnection()
    broker = Broker(config={"query_timeout_seconds": 7}, channels=("metrics",), sync_connection=connection)

    broker.publish_message(channel="metrics", message="payload")

    assert connection.timeout == 7


def test_connection_query_timeout_must_be_positive():
    with pytest.raises(ValueError, match="query_timeout_seconds must be greater than zero"):
        Broker(config={"query_timeout_seconds": 0}, channels=("metrics",))


def test_connection_timeout_must_be_positive():
    with pytest.raises(ValueError, match="connect_timeout_seconds must be greater than zero"):
        Broker(config={"connect_timeout_seconds": 0}, channels=("metrics",))


def test_publish_uses_configured_default_channel():
    connection = FakeConnection()
    broker = Broker(config={}, channels=("metrics", "dashboard"), default_publish_channel="metrics", sync_connection=connection)

    channel = broker.publish_message(message='{"task":"metrics.collect"}')

    assert channel == "metrics"
    assert connection.cursor_object.calls[0][1] == ("metrics", '{"task":"metrics.collect"}')


def test_publish_uses_sole_channel_as_default():
    connection = FakeConnection()
    broker = Broker(config={}, channels=("metrics",), sync_connection=connection)

    assert broker.publish_message(message="payload") == "metrics"


def test_receive_parameterizes_multiple_channels_and_returns_rows_in_id_order():
    connection = FakeConnection(rows=[(2, "dashboard", "second"), (1, "metrics", "first")])
    broker = Broker(config={}, channels=("metrics", "dashboard"), sync_connection=connection)

    messages = list(broker.process_notify(timeout=0.1, max_messages=2))

    query, params = connection.cursor_object.calls[0]
    assert "SELECT TOP (?) id, channel, payload FROM dbo.dispatcherd_messages" in query
    assert "UPDLOCK" in query
    assert "READPAST" in query
    assert "OUTPUT deleted.id, deleted.channel, deleted.payload" in query
    assert params == (2, "metrics", "dashboard")
    assert messages == [("metrics", "first"), ("dashboard", "second")]


def test_receive_with_no_channels_does_not_issue_invalid_query():
    connection = FakeConnection()
    broker = Broker(config={}, channels=(), sync_connection=connection)

    assert list(broker.process_notify(timeout=0.01)) == []
    assert connection.cursor_object.calls == []


def test_receive_preserves_large_unicode_payload_without_chunking():
    payload = "metrics ☃ " * 1000
    connection = FakeConnection(rows=[(7, "metrics", payload)])
    broker = Broker(config={}, channels=("metrics",), sync_connection=connection)

    messages = list(broker.process_notify(timeout=0.1, max_messages=1))

    assert messages == [("metrics", payload)]


@pytest.mark.asyncio
async def test_async_listener_calls_ready_callback_and_publishes_reply_on_one_executor_thread():
    connection = FakeConnection(rows=[(1, "metrics", "incoming")])
    broker = Broker(config={}, channels=("metrics",), async_connection=connection, poll_interval_seconds=0.01)
    callback_calls = []

    async def connected_callback():
        callback_calls.append("connected")

    listener = broker.aprocess_notify(connected_callback=connected_callback)

    assert await anext(listener) == ("metrics", "incoming")
    await broker.apublish_message(channel="metrics", message="reply")
    await listener.aclose()

    assert callback_calls == ["connected"]
    assert connection.cursor_object.calls[1] == (
        "INSERT INTO dbo.dispatcherd_messages (channel, payload) VALUES (?, ?)",
        ("metrics", "reply"),
    )
    assert len(set(connection.cursor_object.execute_thread_ids)) == 1
    assert connection.cursor_object.execute_thread_ids[0] != threading.get_ident()
    await broker.aclose()
    assert connection.closed is False


@pytest.mark.asyncio
async def test_async_listener_wait_is_cancellable_and_closes_owned_connection():
    connection = FakeConnection()
    broker = Broker(config={}, channels=("metrics",), async_connection_factory=lambda **_: connection, poll_interval_seconds=0.01)
    listener = broker.aprocess_notify()
    next_message = asyncio.create_task(anext(listener))

    await asyncio.sleep(0.02)
    next_message.cancel()
    with pytest.raises(asyncio.CancelledError):
        await next_message

    await broker.aclose()

    assert connection.closed is True


@pytest.mark.asyncio
async def test_aclose_waits_for_cancelled_connection_creation_and_closes_owned_connection():
    connection = FakeConnection()
    connect_started = threading.Event()
    allow_connect_to_finish = threading.Event()

    def connect(**_):
        connect_started.set()
        assert allow_connect_to_finish.wait(timeout=5)
        return connection

    broker = Broker(config={}, channels=("metrics",), async_connection_factory=connect)
    connect_task = asyncio.create_task(broker._get_async_connection())
    assert await asyncio.to_thread(connect_started.wait, 1)
    connect_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connect_task

    close_task = asyncio.create_task(broker.aclose())
    await asyncio.sleep(0.01)
    assert not close_task.done()
    allow_connect_to_finish.set()

    await close_task

    assert connection.closed is True


@pytest.mark.asyncio
async def test_aclose_finishes_connection_cleanup_when_cancelled():
    connection = FakeConnection()
    connect_started = threading.Event()
    allow_connect_to_finish = threading.Event()

    def connect(**_):
        connect_started.set()
        assert allow_connect_to_finish.wait(timeout=5)
        return connection

    broker = Broker(config={}, channels=("metrics",), async_connection_factory=connect)
    connect_task = asyncio.create_task(broker._get_async_connection())
    assert await asyncio.to_thread(connect_started.wait, 1)
    close_task = asyncio.create_task(broker.aclose())
    await asyncio.sleep(0.01)
    close_task.cancel()
    allow_connect_to_finish.set()

    with pytest.raises(asyncio.CancelledError):
        await close_task
    with pytest.raises(RuntimeError, match="closed"):
        await broker._get_async_connection()

    assert await connect_task is connection
    assert connection.closed is True


@pytest.mark.asyncio
async def test_async_publish_surfaces_driver_errors():
    connection = FakeConnection()
    connection.cursor_object.execute_error = RuntimeError("database unavailable")
    broker = Broker(config={}, channels=("metrics",), async_connection=connection)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await broker.apublish_message(channel="metrics", message="payload")

    await broker.aclose()
    assert connection.closed is False
