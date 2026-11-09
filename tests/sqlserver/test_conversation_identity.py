"""Conversation identity, transient self-checks and real producer recovery."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dispatcherd import factories  # noqa: F401 - initialize control/factories cycle
from dispatcherd.control import Control
from dispatcherd.producers.brokered import BrokeredProducer
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.main import DispatcherMain

pytest.importorskip('pyodbc', exc_type=ImportError)
pytest.importorskip('aioodbc')
from test_integration import channel  # noqa: E402 - import after optional dependency checks

from dispatcherd.brokers import sqlserver  # noqa: E402 - import after optional dependency checks
from dispatcherd.brokers.sqlserver import _arows, _rows, connect  # noqa: E402 - import after optional dependency checks


def test_identity_and_subscription_schema(config, brokers):
    channels = [channel(), channel()]
    listener = brokers(channels)
    identity = listener.register()
    connection = connect(config)
    try:
        columns = _rows(connection, "SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.DispatcherdListenerRegistry')")
        assert {r['name'] for r in columns} == {'conversation_id', 'node_id', 'created_at'}
        rows = _rows(connection, 'SELECT conversation_id FROM dbo.DispatcherdSubscription WHERE channel=?', channels[0])
        assert [str(r['conversation_id']).lower() for r in rows] == [identity]
        endpoints = _rows(connection, 'SELECT conversation_id FROM sys.conversation_endpoints WHERE conversation_id=?', identity)
        assert len(endpoints) == 2
        assert _rows(connection, "SELECT OBJECT_ID('dbo.DispatcherdReady') AS obsolete")[0]['obsolete'] is None
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_publication_does_not_wait_for_initial_check(config, brokers, monkeypatch):
    listener = brokers([channel()])
    registered, release = asyncio.Event(), asyncio.Event()
    real = sqlserver._arows

    async def pause_after_register(connection, statement, *args):
        rows = await real(connection, statement, *args)
        if 'DispatcherdRegister ' in statement:
            registered.set()
            await release.wait()
        return rows

    monkeypatch.setattr(sqlserver, '_arows', pause_after_register)
    task = asyncio.create_task(listener.aregister())
    try:
        await asyncio.wait_for(registered.wait(), 5)
        publisher = brokers()
        targets = await publisher.abroadcast(listener.channels[0], 'queued before check')
        assert len(targets) == 1
        release.set()
        assert await task == targets[0]
        assert await listener._areceive(100) == (listener.channels[0], 'queued before check')
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_cancel_after_registration_commit_cleans_identity(brokers, monkeypatch):
    listener = brokers([channel()])
    registered, release = asyncio.Event(), asyncio.Event()
    real = sqlserver._arows
    identities = []

    async def pause_after_register(connection, statement, *args):
        rows = await real(connection, statement, *args)
        if 'DispatcherdRegister ' in statement:
            identities.append(str(rows[0]['instance_id']).lower())
            registered.set()
            await release.wait()
        return rows

    monkeypatch.setattr(sqlserver, '_arows', pause_after_register)
    task = asyncio.create_task(listener.aregister())
    await asyncio.wait_for(registered.wait(), 5)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert listener.instance_id is None
    assert not set(identities) & {r['instance_id'] for r in await brokers().asnapshot()}


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['connection', 'missing_check'])
async def test_real_producer_recycles_and_replies(config, brokers, monkeypatch, failure):
    listener = brokers([channel()], max_connection_idle_seconds=0.15, max_self_check_message_age_seconds=0.15)
    # Disable ODBC's transparent idle-connection recovery so KILL exercises
    # dispatcherd's recovery path rather than being hidden by the native driver.
    listener.config['connection_string'] += ';ConnectRetryCount=0;'
    shared = SharedAsyncObjects()
    producer = BrokeredProducer(listener, shared)
    dispatcher = DispatcherMain(
        producers=[producer],
        shared=shared,
        pool=SimpleNamespace(start_working=AsyncMock(), shutdown=AsyncMock()),
        node_id='recovered',
    )
    real = sqlserver._arows
    try:
        await dispatcher.start_working()
        await asyncio.wait_for(producer.events.ready_event.wait(), 5)
        old_identity = listener.instance_id
        old_connection = listener._async_listener
        assert listener.get_self_check_stats()['success_count'] == 1
        # The actual service loop consumes periodic checks without submitting tasks.
        async with asyncio.timeout(5):
            while listener.get_self_check_stats()['success_count'] < 2:
                await asyncio.sleep(0.02)
        assert dispatcher.received_count == 0

        if failure == 'connection':
            async with listener._async_listen_lock:
                spid = (await _arows(listener._async_listener, 'SELECT @@SPID AS spid'))[0]['spid']
            admin = connect(config)
            try:
                _rows(admin, f'KILL {int(spid)}')
            finally:
                admin.close()
        else:

            async def lose_check(connection, statement, *args):
                if statement == 'SELECT CAST(? AS nvarchar(36)) AS token':
                    return []
                return await real(connection, statement, *args)

            monkeypatch.setattr(sqlserver, '_arows', lose_check)

        await asyncio.wait_for(producer.events.recycle_event.wait(), 5)
        monkeypatch.setattr(sqlserver, '_arows', real)
        producer.events.ready_event.clear()
        await dispatcher.recycle_broker_producers()
        await asyncio.wait_for(producer.events.ready_event.wait(), 5)
        assert producer.recycle_count == 1
        assert listener.instance_id != old_identity
        assert listener._async_listener is not old_connection
        assert old_identity not in {r['instance_id'] for r in await brokers().asnapshot()}
        control = Control('sqlserver', {'config': config, 'default_publish_channel': listener.channels[0], 'wait_timeout_ms': 100})
        replies = await control.acontrol_with_reply('alive', expected_replies=1, timeout=3)
        assert [r['node_id'] for r in replies] == ['recovered']
    finally:
        monkeypatch.setattr(sqlserver, '_arows', real)
        await dispatcher.shutdown()


@pytest.mark.parametrize('idle', [0.05, None])
def test_sync_self_checks_are_internal(brokers, idle):
    listener = brokers([channel()], max_connection_idle_seconds=idle, max_self_check_message_age_seconds=0.2)
    assert list(listener.process_notify(timeout=0.25, max_messages=None)) == []
    successes = listener.get_self_check_stats()['success_count']
    assert successes == 1 if idle is None else successes >= 2
    assert listener.instance_id is None


@pytest.mark.asyncio
async def test_initial_check_failure_cleans_registration(brokers, monkeypatch):
    listener = brokers([channel()], registration_timeout=0.1)
    real = sqlserver._arows

    async def discard_check(connection, statement, *args):
        rows = await real(connection, statement, *args)
        if 'DispatcherdReceive ' in statement:
            return []  # The message left SQL Server but never reached the verifier.
        return rows

    monkeypatch.setattr(sqlserver, '_arows', discard_check)
    with pytest.raises(ConnectionError, match='self-check timed out'):
        await listener.aregister()
    assert listener.instance_id is None
    assert await brokers().asnapshot(listener.channels[0]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize('commit', [True, False])
async def test_periodic_connectivity_while_publication_is_uncommitted(config, brokers, commit):
    topic = channel()
    listener = brokers([topic], max_connection_idle_seconds=0.05, max_self_check_message_age_seconds=0.5)
    await listener.aregister()
    identity = listener.instance_id
    connection = connect({**config, 'autocommit': False})
    publisher = sqlserver.Broker(config, sync_connection=connection)
    received = []

    async def listen():
        async for item in listener.aprocess_notify():
            received.append(item)

    task = asyncio.create_task(listen())
    try:
        publisher.publish_message(topic, 'pending')
        initial = listener.get_self_check_stats()['success_count']
        async with asyncio.timeout(3):
            while listener.get_self_check_stats()['success_count'] < initial + 3:
                await asyncio.sleep(0.02)
        assert listener.instance_id == identity
        assert received == []
        if commit:
            connection.commit()
            async with asyncio.timeout(3):
                while not received:
                    await asyncio.sleep(0.02)
            assert received == [(topic, 'pending')]
        else:
            connection.rollback()
            await asyncio.sleep(0.2)
            assert received == []
    finally:
        connection.rollback()
        connection.close()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_receive_restores_session_lock_timeout(brokers):
    listener = brokers([channel()])
    listener.register()
    _rows(listener._listener, 'SET LOCK_TIMEOUT 9876')
    assert listener._receive(1) is None
    assert _rows(listener._listener, 'SELECT @@LOCK_TIMEOUT AS timeout')[0]['timeout'] == 9876


@pytest.mark.asyncio
@pytest.mark.parametrize('async_client', [False, True])
async def test_retirement_failure_preserves_listener_error(config, brokers, monkeypatch, caplog, async_client):
    listener = brokers([channel()])
    if async_client:
        await listener.aregister()
    else:
        listener.register()
    identity = listener.instance_id

    def fail_retire(*args):
        raise RuntimeError('retirement unavailable')

    async def afail_retire(*args):
        fail_retire()

    def failed_callback():
        raise ConnectionError('original listener failure')

    async def afailed_callback():
        failed_callback()

    monkeypatch.setattr(listener, 'retire', fail_retire)
    monkeypatch.setattr(listener, 'aretire', afail_retire)
    try:
        with pytest.raises(ConnectionError, match='original listener failure'):
            if async_client:
                async for _ in listener.aprocess_notify(connected_callback=afailed_callback):
                    pass
            else:
                list(listener.process_notify(connected_callback=failed_callback))
        assert listener.instance_id is None
        assert listener._listener is None
        assert listener._async_listener is None
        assert identity in caplog.text
        assert 'explicit cleanup may be required' in caplog.text
    finally:
        sqlserver.Broker(config).retire([identity])


def test_failed_registration_rolls_back_owned_transaction(config):
    connection = connect(config)
    try:
        with pytest.raises(sqlserver.pyodbc.Error):
            _rows(connection, 'EXEC dbo.DispatcherdRegister ?,?,?', None, '["duplicate","duplicate"]', b'{}')
        assert _rows(connection, 'SELECT @@TRANCOUNT AS count')[0]['count'] == 0
    finally:
        connection.close()


def test_reply_receive_restores_lock_timeout_on_error(brokers):
    listener = brokers()
    listener.open_reply_endpoint(timeout=2)
    _rows(listener._listener, 'SET LOCK_TIMEOUT 9876')
    assert listener._receive(1) is None
    assert _rows(listener._listener, 'SELECT @@LOCK_TIMEOUT AS timeout')[0]['timeout'] == 9876
    sql, _ = listener._receive_query(1)
    with pytest.raises(sqlserver.pyodbc.Error):
        _rows(listener._listener, sql, '00000000-0000-0000-0000-000000000000', 1)
    assert _rows(listener._listener, 'SELECT @@LOCK_TIMEOUT AS timeout')[0]['timeout'] == 9876


@pytest.mark.asyncio
@pytest.mark.parametrize('async_client', [False, True])
async def test_publication_diagnostics(brokers, monkeypatch, caplog, async_client):
    topic = channel()
    publisher = brokers(max_message_bytes=1024)
    if async_client:
        await publisher.apublish_message(topic, 'secret body')
    else:
        publisher.publish_message(topic, 'secret body')
    assert 'has no recipients' in caplog.text
    assert topic in caplog.text
    caplog.clear()
    listener = brokers([topic])
    identity = listener.register()
    real_rows, real_arows = sqlserver._rows, sqlserver._arows
    sends = 0

    def after_send(statement):
        nonlocal sends
        if 'EXEC dbo.DispatcherdSend ' in statement:
            sends += 1
            if sends == 1:
                listener.retire([identity])

    def rows(connection, statement, *args):
        result = real_rows(connection, statement, *args)
        after_send(statement)
        return result

    async def arows(connection, statement, *args):
        result = await real_arows(connection, statement, *args)
        after_send(statement)
        return result

    monkeypatch.setattr(sqlserver, '_rows', rows)
    monkeypatch.setattr(sqlserver, '_arows', arows)
    if async_client:
        assert await publisher.abroadcast(topic, 'secret body' * 300) == []
    else:
        assert publisher.broadcast(topic, 'secret body' * 300) == []
    assert 'send rejected' in caplog.text
    assert 'chunk=2/' in caplog.text
    assert identity in caplog.text
    assert 'secret body' not in caplog.text
