import asyncio
import json
import subprocess
import sys
import uuid
from types import SimpleNamespace

import pytest

# Import factories first: upstream control/factories have a circular import.
from dispatcherd import factories  # noqa: F401 - initialize upstream control/factories cycle
from dispatcherd.chunking import ChunkAccumulator
from dispatcherd.control import Control
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.main import DispatcherMain

pytest.importorskip("pyodbc", exc_type=ImportError)
pytest.importorskip('aioodbc')
from dispatcherd.brokers.sqlserver import Broker, _rows, connect  # noqa: E402 - import after optional dependency checks


def channel():
    return "test_" + uuid.uuid4().hex


def receive(broker):
    return broker._receive(200)


def resources(config, handles):
    connection = connect(config)
    try:
        return {
            "endpoints": _rows(
                connection,
                "SELECT state FROM sys.conversation_endpoints WHERE conversation_handle IN (?,?) AND state <> 'CD'",
                *handles,
            ),
            "inbox": _rows(
                connection,
                "SELECT conversation_handle FROM dbo.DispatcherdInbox WHERE conversation_handle IN (?,?)",
                *handles,
            ),
            "outbox": _rows(
                connection,
                "SELECT conversation_handle FROM dbo.DispatcherdOutbox WHERE conversation_handle IN (?,?)",
                *handles,
            ),
            "transmission": _rows(
                connection,
                "SELECT conversation_handle FROM sys.transmission_queue WHERE conversation_handle IN (?,?)",
                *handles,
            ),
        }
    finally:
        connection.close()


def test_setup_idempotent(config):
    Broker.setup_database(config)
    Broker.setup_database(config)


@pytest.mark.parametrize("count", [1, 2, 4])
def test_broadcast_independent(brokers, count):
    c = channel()
    listeners = [brokers([c]) for _ in range(count)]
    for listener in listeners:
        listener.register()
    publisher = brokers()
    assert len(publisher.broadcast(c, '{"task":"hello"}')) == count
    assert [receive(b) for b in listeners] == [(c, '{"task":"hello"}')] * count
    assert all(receive(b) is None for b in listeners)


def test_anycast_and_target(brokers):
    c = channel()
    listeners = [brokers([c]) for _ in range(3)]
    for listener in listeners:
        listener.register()
    publisher = brokers()
    selected = publisher.send_any(c, "one")
    assert sum(receive(b) is not None for b in listeners) == 1
    assert selected in {b.instance_id for b in listeners}
    assert publisher.send_to(listeners[1].instance_id, c, "target")
    assert [receive(b) for b in listeners] == [None, (c, "target"), None]
    assert not publisher.send_to(str(uuid.uuid4()), c, "missing")
    assert publisher.send_any(channel(), "missing") is None


def test_overlapping_channels_and_defaults(brokers):
    a, b, c = channel(), channel(), channel()
    listeners = [brokers([a, b]), brokers([b, c]), brokers([c])]
    for listener in listeners:
        listener.register()
    publisher = brokers(default_publish_channel=b)
    assert publisher.publish_message(message="both") == b
    assert [receive(x) for x in listeners] == [(b, "both"), (b, "both"), None]
    listeners[2].publish_message(message="default")
    assert receive(listeners[1]) == (c, "default")
    assert receive(listeners[2]) == (c, "default")
    assert not publisher.send_to(listeners[0].instance_id, c, "wrong subscription")


@pytest.mark.asyncio
async def test_async_publish_and_scheduling(brokers):
    c = channel()
    b = brokers([c])
    iterator = b.aprocess_notify()
    # Register explicitly so we can coordinate without sleeps.
    await b.aregister()
    pending = asyncio.create_task(anext(iterator))
    ticks = 0
    for _ in range(20):
        await asyncio.sleep(0.005)
        ticks += 1
    assert ticks == 20 and not pending.done()
    await b.apublish_message(message="async")
    assert await asyncio.wait_for(pending, 3) == (c, "async")
    await iterator.aclose()
    assert b.instance_id is None


@pytest.mark.parametrize("commit", [True, False])
def test_transaction_visibility(config, brokers, commit):
    c = channel()
    listener = brokers([c])
    listener.register()
    connection = connect({**config, "autocommit": False})
    publisher = Broker(config, sync_connection=connection)
    try:
        publisher.publish_message(c, "transaction")
        assert receive(listener) is None
        if commit:
            connection.commit()
        else:
            connection.rollback()
        assert receive(listener) == ((c, "transaction") if commit else None)
        publisher.close()
        assert _rows(connection, "SELECT 1 AS ok")[0]["ok"] == 1
    finally:
        connection.close()


async def service(broker, node_id, ready):
    dispatcher = DispatcherMain(
        producers=[],
        pool=SimpleNamespace(),
        shared=SharedAsyncObjects(),
        node_id=node_id,
    )

    async def connected():
        ready.set()

    async for c, message in broker.aprocess_notify(connected):
        reply_to, payload = await dispatcher.process_message(message, channel=c)
        if reply_to:
            await broker.apublish_message(reply_to, message=payload)


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.asyncio
async def test_real_control_multiple_replies(config, brokers, sync):
    c = channel()
    listeners = [brokers([c]) for _ in range(3)]
    ready = [asyncio.Event() for _ in listeners]
    tasks = [asyncio.create_task(service(b, f"node-{i}", r)) for i, (b, r) in enumerate(zip(listeners, ready))]
    control = Control(
        "sqlserver",
        {"config": config, "default_publish_channel": c, "wait_timeout_ms": 100},
    )
    try:
        await asyncio.wait_for(asyncio.gather(*(r.wait() for r in ready)), 10)
        if sync:
            result = await asyncio.to_thread(control.control_with_reply, "alive", expected_replies=3, timeout=3)
        else:
            result = await control.acontrol_with_reply("alive", expected_replies=3, timeout=3)
        assert {row["node_id"] for row in result} == {"node-0", "node-1", "node-2"}
        assert not [r for r in await asyncio.to_thread(listeners[0].snapshot) if any(c.startswith("reply_to_") for c in r["channels"])]
    finally:
        for task in tasks:
            task.cancel()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [e for e in outcomes if isinstance(e, Exception)]
        assert not errors, repr(errors)


@pytest.mark.asyncio
async def test_control_timeout_cleans_reply(config, brokers):
    control = Control(
        "sqlserver",
        {
            "config": config,
            "default_publish_channel": channel(),
            "wait_timeout_ms": 100,
        },
    )
    assert await control.acontrol_with_reply("alive", timeout=0.2) == []
    assert not [r for r in brokers().snapshot() if any(c.startswith("reply_to_") for c in r["channels"])]


def test_graceful_cleanup_discards_pending(config, brokers):
    c = channel()
    b = brokers([c])
    instance = b.register()
    row = b.snapshot(c)[0]
    assert str(row["publisher_handle"]) != str(row["receiver_handle"])
    for _ in range(5):
        b.publish_message(c, "pending")
    b.close()
    observer = brokers()
    assert instance not in {r["instance_id"] for r in observer.snapshot()}
    assert not any(resources(config, (row["publisher_handle"], row["receiver_handle"])).values())


@pytest.mark.asyncio
async def test_crash_probe_retire_restart(config, brokers):
    c = channel()
    code = """import json,os
from dispatcherd.brokers.sqlserver import Broker
b=Broker({"connection_string": os.environ["TEST_CONNECTION_STRING"]}, channels=[os.environ['TEST_CHANNEL']])
b.register()
print(json.dumps(b.snapshot(os.environ['TEST_CHANNEL'])[0], default=str),flush=True)
os._exit(0)
"""
    import os

    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", code],
        env={**os.environ, "TEST_CHANNEL": c, "TEST_CONNECTION_STRING": config["connection_string"]},
        capture_output=True,
        text=True,
        check=True,
    )
    dead = json.loads(result.stdout)
    publisher = brokers()
    for _ in range(3):
        assert await publisher.asend_to(dead["instance_id"], c, "discard me")
    live = brokers([c])
    ready = asyncio.Event()
    task = asyncio.create_task(service(live, "live", ready))
    try:
        await asyncio.wait_for(ready.wait(), 10)
        report = await publisher.probe(timeout=0.5)
        assert dead["instance_id"] in report["suspected_stale"]
        assert live.instance_id in report["responded"]
        await publisher.aretire([dead["instance_id"]])
        assert not any(resources(config, (dead["publisher_handle"], dead["receiver_handle"])).values())
        restarted = brokers([c])
        await restarted.aregister()
        assert restarted.instance_id != dead["instance_id"]
        assert await restarted._areceive(200) is None
        assert not await publisher.asend_to(dead["instance_id"], c, "old lifetime")
        assert await publisher.asend_to(restarted.instance_id, c, "new lifetime")
        assert await restarted._areceive(200) == (c, "new lifetime")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_registration_and_publish_races(brokers):
    c = channel()
    listeners = [brokers([c]) for _ in range(5)]
    await asyncio.gather(*(b.aregister() for b in listeners))
    publishers = [brokers() for _ in range(4)]
    await asyncio.gather(*(p.abroadcast(c, f"publisher-{i}") for i, p in enumerate(publishers)))
    for b in listeners:
        messages = [await b._areceive(200) for _ in publishers]
        assert {m for _, m in messages} == {f"publisher-{i}" for i in range(4)}
        assert all(ch == c for ch, _ in messages)


@pytest.mark.asyncio
async def test_repeated_concurrent_channel_isolation(brokers):
    channels = [channel(), channel()]
    listeners = [brokers([c]) for c in channels]
    await asyncio.gather(*(b.aregister() for b in listeners))
    publishers = [brokers() for _ in range(4)]
    for n in range(5):
        await asyncio.gather(*(p.apublish_message(channels[i % 2], message=f"{i}:{n}") for i, p in enumerate(publishers)))
    for i, b in enumerate(listeners):
        got = [await b._areceive(200) for _ in range(10)]
        assert all(c == channels[i] for c, _ in got)
        assert {m for _, m in got} == {f"{j}:{n}" for j in range(4) if j % 2 == i for n in range(5)}


def test_chunking_unicode_anycast(brokers):
    c = channel()
    listeners = [brokers([c]) for _ in range(2)]
    for b in listeners:
        b.register()
    publisher = brokers(max_message_bytes=1024)
    message = {"task": "lambda: None", "args": ["雪🐈" * 2000]}
    selected = publisher.send_any(c, json.dumps(message, ensure_ascii=False))
    b = next(b for b in listeners if b.instance_id == selected)
    accumulator = ChunkAccumulator()
    assembled = None
    while assembled is None:
        result = receive(b)
        assert result is not None
        _, assembled = accumulator.ingest_dict(json.loads(result[1]))
    assert assembled == message
    other = next(x for x in listeners if x is not b)
    assert receive(other) is None


@pytest.mark.asyncio
async def test_cancel_blocked_receive_and_reuse(brokers):
    b = brokers([channel()])
    await b.aregister()
    old = b.instance_id
    iterator = b.aprocess_notify()
    pending = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0.02)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert b.instance_id is None
    assert old not in {r["instance_id"] for r in b.snapshot()}
    await b.aregister()
    assert b.instance_id != old


def test_sync_callback_readiness(brokers):
    c = channel()
    listener = brokers([c])
    publisher = brokers()
    got = list(listener.process_notify(lambda: publisher.publish_message(c, "ready"), timeout=1))
    assert got == [(c, "ready")]
    assert listener.instance_id is None


def test_system_enddialog_not_application(config, brokers):
    c = channel()
    listener = brokers([c])
    listener.register()
    row = listener.snapshot(c)[0]
    connection = connect(config)
    try:
        _rows(
            connection,
            "DECLARE @h uniqueidentifier=?; END CONVERSATION @h;",
            row["publisher_handle"],
        )
        with pytest.raises(ConnectionError, match="EndDialog"):
            receive(listener)
        assert listener.instance_id is None
    finally:
        connection.close()


def test_runtime_role_has_no_ddl(config):
    connection = connect(config)
    try:
        _rows(
            connection,
            "IF USER_ID('dispatcherd_test_runtime') IS NULL CREATE USER dispatcherd_test_runtime WITHOUT LOGIN; ALTER ROLE "
            "dispatcherd_runtime ADD MEMBER dispatcherd_test_runtime",
        )
        _rows(connection, "EXECUTE AS USER='dispatcherd_test_runtime'")
        assert (
            _rows(
                connection,
                "SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CREATE TABLE') AS allowed",
            )[
                0
            ]["allowed"]
            == 0
        )
        identity = _rows(
            connection,
            "EXEC dbo.DispatcherdRegister ?,?,?",
            "unprivileged",
            json.dumps([channel()]),
            b'{"kind":"self_check","token":"test"}',
        )[
            0
        ]['instance_id']
        _rows(connection, "EXEC dbo.DispatcherdRetire ?", identity)
        _rows(connection, "REVERT")
    finally:
        connection.close()


def test_partial_registration_can_be_retired(config, brokers):
    c = channel()
    connection = connect(config)
    try:
        identity = str(
            _rows(
                connection,
                "EXEC dbo.DispatcherdRegister ?,?,?",
                "crash-before-check",
                json.dumps([c]),
                b'{"kind":"self_check","token":"test"}',
            )[
                0
            ]['instance_id']
        ).lower()
        observer = brokers()
        row = observer.snapshot(c)[0]
        # No receipt/readiness gate: even before the initial check is consumed,
        # the conversation is a valid publication destination.
        assert observer.send_to(identity, c, "before initial check")
        observer.retire([identity])
        assert observer.snapshot(c) == []
        assert (
            _rows(
                connection,
                "SELECT state FROM sys.conversation_endpoints WHERE conversation_id=(SELECT conversation_id FROM "
                "sys.conversation_endpoints WHERE conversation_handle=?) AND state<>'CD'",
                row["publisher_handle"],
            )
            == []
        )
    finally:
        connection.close()


def test_closed_recipient_does_not_break_broadcast(config, brokers):
    c = channel()
    stale, live = brokers([c]), brokers([c])
    stale.register()
    live.register()
    row = next(r for r in stale.snapshot(c) if r["instance_id"] == stale.instance_id)
    connection = connect(config)
    try:
        _rows(
            connection,
            "DECLARE @h uniqueidentifier=?; END CONVERSATION @h;",
            row["publisher_handle"],
        )
        publisher = brokers()
        assert publisher.broadcast(c, "survivor") == [live.instance_id]
        assert receive(live) == (c, "survivor")
    finally:
        connection.close()


def test_factory_connection_ownership(config, brokers):
    c = channel()
    listener = brokers([c])
    listener.register()
    connection = connect(config)
    calls = []

    def factory(**options):
        calls.append(options)
        return connection

    publisher = Broker(config, sync_connection_factory=factory)
    try:
        publisher.publish_message(c, "factory")
        publisher.close()
        assert receive(listener) == (c, "factory")
        assert len(calls) == 1
        assert _rows(connection, "SELECT 1 AS ok")[0]["ok"] == 1
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_registration_cancelled_before_callback(brokers):
    broker = brokers([channel()])
    entered = asyncio.Event()

    async def callback():
        entered.set()
        await asyncio.Event().wait()

    iterator = broker.aprocess_notify(callback)
    pending = asyncio.create_task(anext(iterator))
    await asyncio.wait_for(entered.wait(), 10)
    identity = broker.instance_id
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert identity not in {r["instance_id"] for r in broker.snapshot()}


def test_runtime_does_not_provision(config):
    connection = connect(config)
    name = "dispatcherd_empty_" + uuid.uuid4().hex
    try:
        _rows(connection, f"CREATE DATABASE [{name}]")
        empty = dict(config)
        empty["connection_string"] = empty["connection_string"].replace("DATABASE={dispatcherd}", f"DATABASE={{{name}}}")
        broker = Broker(empty, channels=["test"])
        try:
            with pytest.raises(Exception, match="DispatcherdSnapshot"):
                broker.publish_message(message="no implicit setup")
        finally:
            broker.close()
        disabled = connect(empty)
        try:
            _rows(disabled, f"ALTER DATABASE [{name}] SET DISABLE_BROKER")
            with pytest.raises(RuntimeError, match="disabled"):
                Broker.setup_database(empty)
        finally:
            disabled.close()
    finally:
        _rows(
            connection,
            f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]",
        )
        connection.close()


def test_register_is_idempotent_without_consuming_messages(config, brokers):
    c = channel()
    listener = brokers([c])
    listener.register()
    listener.publish_message(c, "keep application message")
    connection = connect(config)
    try:
        assert listener.register() == listener.instance_id
        assert receive(listener) == (c, "keep application message")
    finally:
        connection.close()


def test_system_error_not_application(config, brokers):
    listener = brokers([channel()])
    listener.register()
    row = next(r for r in listener.snapshot() if r["instance_id"] == listener.instance_id)
    connection = connect(config)
    try:
        _rows(
            connection,
            "DECLARE @h uniqueidentifier=?; END CONVERSATION @h WITH ERROR=50001 DESCRIPTION='test';",
            row["publisher_handle"],
        )
        with pytest.raises(ConnectionError, match="Error"):
            receive(listener)
        assert listener.instance_id is None
        assert not any(resources(config, (row["publisher_handle"], row["receiver_handle"])).values())
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_publish_and_retirement_race(brokers):
    c = channel()
    listeners = [brokers([c]) for _ in range(8)]
    await asyncio.gather(*(b.aregister() for b in listeners))
    publisher = brokers()
    doomed = [b.instance_id for b in listeners[:4]]
    await asyncio.gather(publisher.abroadcast(c, "race"), publisher.aretire(doomed))
    for b in listeners[4:]:
        assert await b._areceive(200) == (c, "race")
    assert not set(doomed) & {r["instance_id"] for r in publisher.snapshot()}


@pytest.mark.asyncio
async def test_cancel_joins_thread_even_when_operation_fails():
    import threading

    from dispatcherd.brokers.sqlserver import _complete_io

    entered = threading.Event()
    release = threading.Event()

    def failing_operation():
        entered.set()
        release.wait(timeout=2)
        raise ConnectionError("connection lost during cancellation")

    async def operation():
        return await asyncio.to_thread(failing_operation)

    task = asyncio.create_task(_complete_io(operation()))
    await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_repeated_transaction_and_retirement_with_default_pooling(config, brokers):
    # Reproduce the reported ordering, including an unfinished SELECT transaction
    # on the borrowed connection. Do not disable process-wide ODBC pooling.
    for commit in [True, False] * 3:
        test_transaction_visibility(config, brokers, commit)
        listener = brokers([channel()])
        listener.register()
        listener.close()
