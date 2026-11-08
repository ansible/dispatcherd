"""Conversation identity, transient self-checks and real producer recovery."""

import asyncio
import json
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
                if 'DispatcherdSend ' in statement and json.loads(args[-1]).get('kind') == 'self_check':
                    return [{'sent': 1}]
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
