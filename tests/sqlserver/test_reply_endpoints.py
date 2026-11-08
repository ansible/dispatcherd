"""Ephemeral direct replies, async-only service I/O, and lifetime/cancellation tests."""

import asyncio
import json
import os
import subprocess
import sys
import time

import pytest

from dispatcherd import factories  # noqa: F401 - initialize control/factories cycle
from dispatcherd.control import Control

pytest.importorskip('pyodbc', exc_type=ImportError)
pytest.importorskip('aioodbc')
from test_integration import channel, resources, service  # noqa: E402 - import after optional dependency checks

from dispatcherd.brokers import sqlserver  # noqa: E402 - import after optional dependency checks
from dispatcherd.brokers.sqlserver import REPLY_PREFIX, Broker, _arows, _rows, connect  # noqa: E402 - import after optional dependency checks


def reply_endpoints(config):
    connection = connect(config)
    try:
        return _rows(
            connection,
            "SELECT conversation_handle,conversation_id,is_initiator,state,lifetime FROM sys.conversation_endpoints WHERE "
            "service_contract_id=(SELECT service_contract_id FROM sys.service_contracts WHERE name='DispatcherdReplyContract') AND "
            "state<>'CD'",
        )
    finally:
        connection.close()


@pytest.mark.parametrize('async_client', [False, True])
@pytest.mark.asyncio
async def test_direct_endpoint_has_no_registration(config, brokers, monkeypatch, async_client):
    reply = brokers()
    publisher = brokers()
    before = await publisher.asnapshot()
    if async_client:
        address = await reply.aopen_reply_endpoint(5)
    else:
        address = await asyncio.to_thread(reply.open_reply_endpoint, 5)
    sender_handle, receiver_handle = reply._reply_publisher, reply.receiver_handle
    assert sender_handle.lower() != receiver_handle.lower()
    assert reply.instance_id is None
    assert await publisher.asnapshot() == before
    assert address.startswith(REPLY_PREFIX)

    original = sqlserver._arows

    async def no_registry(connection, sql, *params):
        assert 'DispatcherdSnapshot' not in sql and 'DispatcherdSend ' not in sql
        return await original(connection, sql, *params)

    monkeypatch.setattr(sqlserver, '_arows', no_registry)
    await publisher.apublish_message(address, message='{"node_id":"direct"}')
    if async_client:
        assert await reply._areceive(200) == (address, '{"node_id":"direct"}')
        await reply.aclose()
    else:
        assert await asyncio.to_thread(reply._receive, 200) == (address, '{"node_id":"direct"}')
        await asyncio.to_thread(reply.close)
    # A reply after timeout/close is an ordinary unavailable destination.
    assert await publisher.abroadcast(address, 'late') == []
    assert not any(resources(config, (sender_handle, receiver_handle)).values())


@pytest.mark.asyncio
async def test_async_service_never_enters_sync_driver_path(config, brokers, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('Async service called synchronous broker/driver code')

    monkeypatch.setattr(sqlserver, 'connect', forbidden)
    monkeypatch.setattr(sqlserver, '_rows', forbidden)
    monkeypatch.setattr(Broker, 'publish_message', forbidden)
    monkeypatch.setattr(Broker, 'register', forbidden)
    c = channel()
    listener = brokers([c])
    ready = asyncio.Event()
    task = asyncio.create_task(service(listener, 'async-only', ready))
    try:
        await asyncio.wait_for(ready.wait(), 5)
        control = Control('sqlserver', {'config': config, 'default_publish_channel': c, 'wait_timeout_ms': 100})
        assert await control.acontrol_with_reply('alive', timeout=5) == [{'node_id': 'async-only'}]
        report = await listener.probe(timeout=1)
        assert report['responded'] == [listener.instance_id]
        assert await listener.acleanup_expired_replies() == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await listener.aclose()


@pytest.mark.asyncio
async def test_concurrent_control_clients_and_chunked_replies(config, brokers, monkeypatch):
    from dispatcherd.service import control_tasks

    async def echo(dispatcher, data):
        return {'token': data['token'], 'large': '雪🐈' * 500}

    monkeypatch.setattr(control_tasks, 'alive', echo)
    c = channel()
    listeners = [brokers([c], max_message_bytes=1024) for _ in range(3)]
    ready = [asyncio.Event() for _ in listeners]
    tasks = [asyncio.create_task(service(b, f'node-{i}', event)) for i, (b, event) in enumerate(zip(listeners, ready))]
    options = {'config': config, 'default_publish_channel': c, 'wait_timeout_ms': 100, 'max_message_bytes': 1024}
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), 5)
        controls = [Control('sqlserver', options) for _ in range(4)]
        results = await asyncio.gather(
            *(
                (
                    asyncio.to_thread(control.control_with_reply, 'alive', expected_replies=3, timeout=10, data={'token': str(i)})
                    if i % 2
                    else control.acontrol_with_reply('alive', expected_replies=3, timeout=10, data={'token': str(i)})
                )
                for i, control in enumerate(controls)
            )
        )
        for i, result in enumerate(results):
            assert {r['node_id'] for r in result} == {'node-0', 'node-1', 'node-2'}
            assert all(r['token'] == str(i) and r['large'] == '雪🐈' * 500 for r in result)
        assert {r['instance_id'] for r in await listeners[0].asnapshot()} == {b.instance_id for b in listeners}
        assert reply_endpoints(config) == []
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_explicit_expiry_sweep_after_crashed_control_caller(config, brokers):
    # Shorten only this test dialog's server lifetime; production defaults to 300.
    code = '''import json,os,time
from dispatcherd.brokers.sqlserver import connect,_rows
c=connect({'connection_string':os.environ['TEST_CONNECTION_STRING']})
r=_rows(c,'EXEC dbo.DispatcherdOpenReply @lifetime=1')[0]
while True:
 rows=_rows(c,'EXEC dbo.DispatcherdReplyReady ?',r['publisher_handle'])
 if rows:
  r.update(rows[0]); break
 time.sleep(.01)
print(json.dumps(r,default=str),flush=True)
os._exit(0)
'''
    process = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, '-c', code],
        env={**os.environ, 'TEST_CONNECTION_STRING': config['connection_string']},
        capture_output=True,
        text=True,
        check=True,
    )
    dead = json.loads(process.stdout)
    publisher = brokers()
    address = REPLY_PREFIX + dead['publisher_handle']
    await publisher.apublish_message(address, message='pending reply')
    worker = brokers([channel()])
    await worker.aregister()
    live_reply = brokers()
    await live_reply.aopen_reply_endpoint(10)
    await asyncio.sleep(1.2)
    assert await publisher.abroadcast(address, 'expired') == []
    retired = await publisher.acleanup_expired_replies()
    assert dead['publisher_handle'].lower() in {h.lower() for h in retired}
    assert worker.instance_id in {r['instance_id'] for r in await publisher.asnapshot()}
    assert not any(resources(config, (dead['publisher_handle'], dead['receiver_handle'])).values())
    # The sweep must leave unexpired reply conversations usable.
    await publisher.apublish_message(live_reply.reply_address, message='still live')
    assert await live_reply._areceive(200) == (live_reply.reply_address, 'still live')
    assert await publisher.acleanup_expired_replies() == []


def test_sync_sweep_handles_incomplete_reply_handshake(config, brokers):
    connection = connect(config)
    try:
        row = _rows(connection, 'EXEC dbo.DispatcherdOpenReply @lifetime=1')[0]
        time.sleep(1.2)
        retired = brokers().cleanup_expired_replies()
        assert str(row['publisher_handle']).lower() in {h.lower() for h in retired}
        assert _rows(connection, "SELECT state FROM sys.conversation_endpoints WHERE conversation_id=? AND state<>'CD'", row['conversation_id']) == []
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_reply_cancellation_during_waitfor(config, brokers):
    reply = brokers()
    await reply.aopen_reply_endpoint(5)
    handles = (reply._reply_publisher, reply.receiver_handle)
    iterator = reply.aprocess_notify()
    pending = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0.02)
    pending.cancel()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, 2)
    await reply.aclose()
    assert reply.receiver_handle is None
    assert not any(resources(config, handles).values())


@pytest.mark.asyncio
async def test_cancel_endpoint_allocation_cleans_dialog(config, brokers, monkeypatch):
    original = sqlserver._arows
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed(connection, sql, *params):
        rows = await original(connection, sql, *params)
        if sql == 'EXEC dbo.DispatcherdOpenReply':
            entered.set()
            await release.wait()
        return rows

    monkeypatch.setattr(sqlserver, '_arows', delayed)
    reply = brokers()
    operation = asyncio.create_task(reply.aopen_reply_endpoint(5))
    await asyncio.wait_for(entered.wait(), 5)
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert reply_endpoints(config) == []


@pytest.mark.parametrize('timeout', [0, -1, 300, 1000])
@pytest.mark.asyncio
async def test_reply_timeout_ceiling(brokers, timeout):
    reply = brokers()
    with pytest.raises(ValueError, match='300'):
        reply.open_reply_endpoint(timeout)
    with pytest.raises(ValueError, match='300'):
        await reply.aopen_reply_endpoint(timeout)


@pytest.mark.asyncio
async def test_async_borrowed_connection_is_never_committed_or_closed(config, brokers):
    import aioodbc

    connection = await aioodbc.connect(dsn=config['connection_string'], autocommit=False)
    c = channel()
    listener = brokers([c])
    await listener.aregister()
    publisher = Broker(config, async_connection=connection)
    try:
        await publisher.apublish_message(c, message='uncommitted')
        assert await listener._areceive(100) is None
        await connection.rollback()
        assert await listener._areceive(100) is None
        await publisher.apublish_message(c, message='committed')
        await connection.commit()
        assert await listener._areceive(100) == (c, 'committed')
        await publisher.aclose()
        assert not connection.closed
        assert (await _arows(connection, 'SELECT 1 AS ok'))[0]['ok'] == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_async_connection_factory_and_sync_isolation(config, brokers):
    import aioodbc

    connection = await aioodbc.connect(dsn=config['connection_string'], autocommit=True)
    calls = []

    async def factory(**options):
        calls.append(options)
        return connection

    class ForbiddenSyncConnection:
        def cursor(self):
            raise AssertionError('Used synchronous connection for asynchronous publishing')

    publisher = Broker(config, sync_connection=ForbiddenSyncConnection(), async_connection_factory=factory)
    listener = brokers([channel()])
    await listener.aregister()
    try:
        await publisher.apublish_message(listener.channels[0], message='async factory')
        assert await listener._areceive(200) == (listener.channels[0], 'async factory')
        await publisher.aclose()
        assert len(calls) == 1 and not connection.closed
    finally:
        await connection.close()


def test_default_lifetime_and_wrong_contract_address(config, brokers):
    reply = brokers()
    address = reply.open_reply_endpoint(5)
    connection = connect(config)
    try:
        seconds = _rows(
            connection,
            'SELECT DATEDIFF(second,GETUTCDATE(),lifetime) AS remaining FROM sys.conversation_endpoints WHERE conversation_handle=?',
            reply._reply_publisher,
        )[0]['remaining']
        assert 295 <= seconds <= 300
        worker = brokers([channel()])
        worker.register()
        worker_handle = worker.snapshot(worker.channels[0])[0]['publisher_handle']
        publisher = brokers()
        # A token cannot repurpose a worker conversation or the receiving endpoint.
        assert publisher.broadcast(REPLY_PREFIX + str(worker_handle), 'invalid') == []
        assert publisher.broadcast(REPLY_PREFIX + reply.receiver_handle, 'wrong side') == []
        assert publisher.broadcast(address, 'valid')
        assert reply._receive(200) == (address, 'valid')
        assert worker._receive(100) is None
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_close_sync_publisher_does_not_stop_async_listener(brokers):
    broker = brokers([channel()])
    await broker.aregister()
    broker.publish_message(message='sync')
    broker.close()
    assert not broker._stopping.is_set()
    assert await broker._areceive(200) == (broker.channels[0], 'sync')
    await broker.apublish_message(message='async')
    assert await broker._areceive(200) == (broker.channels[0], 'async')


def test_runtime_role_can_use_unregistered_reply_endpoint(config):
    connection = connect(config)
    handle = None
    try:
        _rows(
            connection,
            "IF USER_ID('dispatcherd_reply_test') IS NULL CREATE USER dispatcherd_reply_test WITHOUT LOGIN; ALTER ROLE "
            "dispatcherd_runtime ADD MEMBER dispatcherd_reply_test",
        )
        _rows(connection, "EXECUTE AS USER='dispatcherd_reply_test'")
        row = _rows(connection, 'EXEC dbo.DispatcherdOpenReply')[0]
        handle = row['publisher_handle']
        deadline = time.monotonic() + 5
        ready = []
        while not ready and time.monotonic() < deadline:
            ready = _rows(connection, 'EXEC dbo.DispatcherdReplyReady ?', handle)
            if not ready:
                time.sleep(0.01)
        assert ready
        assert _rows(connection, 'EXEC dbo.DispatcherdSendReply ?,?', handle, b'{"reply":true}')[0]['sent'] == 1
        assert bytes(_rows(connection, 'EXEC dbo.DispatcherdReceiveReply ?,?', ready[0]['receiver_handle'], 100)[0]['message_body']) == b'{"reply":true}'
        _rows(connection, 'EXEC dbo.DispatcherdCloseReply ?', handle)
        assert _rows(connection, 'EXEC dbo.DispatcherdExpiredReplies') == []
    finally:
        _rows(connection, 'REVERT')
        if handle:
            _rows(connection, 'EXEC dbo.DispatcherdCloseReply ?', handle)
        connection.close()
