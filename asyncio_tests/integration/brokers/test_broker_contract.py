"""Behavior shared by database brokers, exercised against a real backend.

Set ``DISPATCHERD_TEST_SQLSERVER_DSN`` to also run against SQL Server when the
broker is available. Backend-specific validation and connection details belong
in their own test modules.
"""

import asyncio
import json
import os
import uuid

import pytest
import pytest_asyncio

from dispatcherd.brokers import get_broker
from dispatcherd.chunking import ChunkAccumulator
from dispatcherd.config import DispatcherSettings
from dispatcherd.factories import get_control_from_settings
from dispatcherd.testing.asyncio import adispatcher_service


class RedactedConfig(dict):
    def __repr__(self):
        return '<SQL Server connection settings; credentials redacted>'


# SQL Server remains opt-in so this module works on branches without that broker.
BACKENDS = ['pg_notify'] + (['sqlserver'] if os.environ.get('DISPATCHERD_TEST_SQLSERVER_DSN') else [])


@pytest.fixture(params=BACKENDS)
def broker_backend(request, conn_config):
    if request.param == 'pg_notify':
        return 'pg_notify', {'config': conn_config, 'max_connection_idle_seconds': None}
    if request.param == 'sqlserver':
        return 'sqlserver', {
            'config': RedactedConfig(connection_string=os.environ['DISPATCHERD_TEST_SQLSERVER_DSN']),
            'wait_timeout_ms': 100,
            'max_connection_idle_seconds': None,
        }
    raise ValueError(f'Unknown broker backend: {request.param}')


@pytest.fixture
def broker_factory(broker_backend):
    name, config = broker_backend
    brokers = []

    def create(*, channels=(), default_publish_channel=None):
        broker = get_broker(name, config, channels=channels, default_publish_channel=default_publish_channel)
        brokers.append(broker)
        return broker

    yield create
    for broker in brokers:
        broker.close()


@pytest_asyncio.fixture
async def async_brokers(broker_factory):
    brokers = []

    def create(**kwargs):
        broker = broker_factory(**kwargs)
        brokers.append(broker)
        return broker

    yield create
    for broker in brokers:
        await broker.aclose()


@pytest.fixture
def channels():
    prefix = 'contract_' + uuid.uuid4().hex
    return [prefix + '_a', prefix + '_b', prefix + '_other']


def test_sync_delivery_routes_channels_and_honors_message_limit(broker_factory, channels):
    listener = broker_factory(channels=channels[:2])
    publisher = broker_factory(default_publish_channel=channels[0])
    callbacks = []

    def publish_after_listen():
        callbacks.append(True)
        publisher.publish_message(message='default')
        publisher.publish_message(channel=channels[2], message='not subscribed')
        publisher.publish_message(channel=channels[1], message='explicit')

    received = list(listener.process_notify(connected_callback=publish_after_listen, timeout=2, max_messages=2))
    assert callbacks == [True]
    assert received == [(channels[0], 'default'), (channels[1], 'explicit')]


def test_sync_timeout_and_reuse_after_close(broker_factory, channels):
    listener = broker_factory(channels=[channels[0]])
    publisher = broker_factory(channels=[channels[0]])
    assert list(listener.process_notify(timeout=0.1, max_messages=1)) == []
    listener.close()
    publisher.close()
    received = list(listener.process_notify(connected_callback=lambda: publisher.publish_message(message='after close'), timeout=2))
    assert received == [(channels[0], 'after close')]


def test_ambiguous_publish_channel_is_rejected(broker_factory, channels):
    publisher = broker_factory(channels=channels[:2])
    with pytest.raises(ValueError):
        publisher.publish_message(message='no default')


@pytest.mark.asyncio
async def test_async_receive_from_sync_and_async_publishers(async_brokers, channels):
    listener = async_brokers(channels=channels[:2])
    publisher = async_brokers(default_publish_channel=channels[0])
    ready = asyncio.Event()
    callbacks = []

    async def on_connected():
        callbacks.append(True)
        ready.set()

    stream = listener.aprocess_notify(connected_callback=on_connected)

    async def receive_two():
        return [await anext(stream), await anext(stream)]

    task = asyncio.create_task(receive_two())
    try:
        await asyncio.wait_for(ready.wait(), 2)
        publisher.publish_message(message='sync')
        await publisher.apublish_message(channel=channels[2], message='not subscribed')
        await publisher.apublish_message(channel=channels[1], message='async')
        assert await asyncio.wait_for(task, 2) == [(channels[0], 'sync'), (channels[1], 'async')]
        assert callbacks == [True]
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await stream.aclose()


@pytest.mark.asyncio
async def test_async_listener_can_publish_reply_while_receiving(async_brokers, channels):
    listener = async_brokers(channels=[channels[0]])
    reply_listener = async_brokers(channels=[channels[1]])
    publisher = async_brokers(default_publish_channel=channels[0])
    ready = asyncio.Event()
    reply_ready = asyncio.Event()

    async def on_connected():
        ready.set()

    async def on_reply_connected():
        reply_ready.set()

    stream = listener.aprocess_notify(connected_callback=on_connected)
    replies = reply_listener.aprocess_notify(connected_callback=on_reply_connected)

    async def receive_and_reply():
        incoming = await anext(stream)
        await listener.apublish_message(channel=channels[1], message='reply')
        # Resume the listener so brokers that queue replies can flush them.
        await anext(stream)
        return incoming

    task = asyncio.create_task(receive_and_reply())
    reply_task = asyncio.create_task(anext(replies))
    try:
        await asyncio.wait_for(asyncio.gather(ready.wait(), reply_ready.wait()), 2)
        await publisher.apublish_message(message='request')
        assert await asyncio.wait_for(reply_task, 2) == (channels[1], 'reply')
        await publisher.apublish_message(message='finish')
        assert await asyncio.wait_for(task, 2) == (channels[0], 'request')
    finally:
        for pending in (task, reply_task):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(task, reply_task, return_exceptions=True)
        await stream.aclose()
        await replies.aclose()


@pytest.mark.asyncio
async def test_async_fanout_to_multiple_listeners(async_brokers, channels):
    listeners = [async_brokers(channels=[channels[0]]) for _ in range(2)]
    publisher = async_brokers(default_publish_channel=channels[0])
    ready = [asyncio.Event(), asyncio.Event()]
    streams = []
    tasks = []
    for listener, event in zip(listeners, ready):

        async def on_connected(event=event):
            event.set()

        stream = listener.aprocess_notify(connected_callback=on_connected)
        streams.append(stream)
        tasks.append(asyncio.create_task(anext(stream)))
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), 2)
        await publisher.apublish_message(message='broadcast')
        assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [(channels[0], 'broadcast')] * 2
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for stream in streams:
            await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('publish_async', [False, True], ids=['sync-publish', 'async-publish'])
async def test_large_unicode_message_reassembles(async_brokers, channels, publish_async):
    listener = async_brokers(channels=[channels[0]])
    publisher = async_brokers(default_publish_channel=channels[0])
    ready = asyncio.Event()
    payload = {'data': '🔥 café "quoted" ' * 2000}
    message = json.dumps(payload, ensure_ascii=False)
    accumulator = ChunkAccumulator()
    received_count = 0

    async def on_connected():
        ready.set()

    stream = listener.aprocess_notify(connected_callback=on_connected)

    async def receive():
        nonlocal received_count
        async for channel, chunk in stream:
            assert channel == channels[0]
            received_count += 1
            _, complete = accumulator.ingest_dict(json.loads(chunk))
            if complete is not None:
                return complete

    task = asyncio.create_task(receive())
    try:
        await asyncio.wait_for(ready.wait(), 2)
        if publish_async:
            await publisher.apublish_message(message=message)
        else:
            publisher.publish_message(message=message)
        assert await asyncio.wait_for(task, 5) == payload
        # A broker may use a larger message limit and deliver this in one piece.
        assert received_count >= 1
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('publish_async', [False, True], ids=['sync-publish', 'async-publish'])
async def test_dispatcher_task_and_control_round_trips(broker_backend, async_brokers, channels, publish_async):
    name, config = broker_backend
    config = {
        'version': 2,
        'brokers': {name: {**config, 'channels': [channels[0]], 'default_publish_channel': channels[0]}},
        'producers': {'ControlProducer': {}},
        'service': {'main_kwargs': {'node_id': 'contract-node'}},
    }
    settings = DispatcherSettings(config)
    control = get_control_from_settings(settings=settings)
    publisher = async_brokers(default_publish_channel=channels[0])

    async with adispatcher_service(config) as dispatcher:
        # Submit a real task through the broker, then observe the worker finish it.
        if publish_async:
            await publisher.apublish_message(message='lambda: "contract task"')
        else:
            publisher.publish_message(message='lambda: "contract task"')
        await asyncio.wait_for(dispatcher.pool.events.work_cleared.wait(), 5)
        assert dispatcher.pool.finished_count == 1

        async_reply = await asyncio.wait_for(control.acontrol_with_reply('alive', timeout=3), 5)
        sync_reply = await asyncio.wait_for(asyncio.to_thread(control.control_with_reply, 'alive', timeout=3), 5)
        assert async_reply == [{'node_id': 'contract-node'}]
        assert sync_reply == [{'node_id': 'contract-node'}]
        assert dispatcher.control_count == 2


@pytest.mark.asyncio
async def test_dispatcher_large_task_and_control_reply(broker_backend, async_brokers, channels):
    name, config = broker_backend
    config = {
        'version': 2,
        'brokers': {name: {**config, 'channels': [channels[0]], 'default_publish_channel': channels[0]}},
        'producers': {'ControlProducer': {}},
        'service': {'main_kwargs': {'node_id': 'contract-node'}},
    }
    control = get_control_from_settings(settings=DispatcherSettings(config))
    publisher = async_brokers(default_publish_channel=channels[0])
    task_uuid = 'large-contract-task'
    large_argument = '🔥' * 5000
    message = json.dumps({'task': 'lambda *args: __import__("time").sleep(10)', 'uuid': task_uuid, 'args': [large_argument]})

    async with adispatcher_service(config):
        await publisher.apublish_message(message=message)

        # Poll a public control command until the submitted task is visible.
        async def running_task():
            while True:
                replies = await control.acontrol_with_reply('running', timeout=2)
                if replies and task_uuid in str(replies):
                    return replies
                await asyncio.sleep(0.02)

        replies = await asyncio.wait_for(running_task(), 5)
        task = next(value for key, value in replies[0].items() if key != 'node_id')
        assert task['uuid'] == task_uuid
        assert task['args'] == [large_argument]
        sync_replies = await asyncio.wait_for(asyncio.to_thread(control.control_with_reply, 'running', timeout=3), 5)
        sync_task = next(value for key, value in sync_replies[0].items() if key != 'node_id')
        assert sync_task['args'] == [large_argument]
        canceled = await asyncio.wait_for(control.acontrol_with_reply('cancel', data={'uuid': task_uuid}, timeout=2), 5)
        canceled_task = next(value for key, value in canceled[0].items() if key != 'node_id')
        assert canceled_task['uuid'] == task_uuid


def test_sync_order_duplicates_and_offline_messages(broker_factory, channels):
    listener = broker_factory(channels=[channels[0]])
    publisher = broker_factory(channels=[channels[0]])
    publisher.publish_message(message='before subscribing')

    def publish():
        for message in ('first', 'duplicate', 'duplicate', 'last'):
            publisher.publish_message(message=message)

    received = list(listener.process_notify(connected_callback=publish, timeout=2, max_messages=4))
    assert received == [(channels[0], message) for message in ('first', 'duplicate', 'duplicate', 'last')]


@pytest.mark.asyncio
async def test_async_publish_requires_an_unambiguous_channel(async_brokers, channels):
    publisher = async_brokers(channels=channels[:2])
    with pytest.raises(ValueError):
        await publisher.apublish_message(message='no default')


@pytest.mark.asyncio
async def test_async_listener_restarts_after_close(async_brokers, channels):
    listener = async_brokers(channels=[channels[0]])
    publisher = async_brokers(channels=[channels[0]])

    for message in ('before close', 'after close'):
        ready = asyncio.Event()

        async def on_connected():
            ready.set()

        stream = listener.aprocess_notify(connected_callback=on_connected)
        task = asyncio.create_task(anext(stream))
        try:
            await asyncio.wait_for(ready.wait(), 2)
            await publisher.apublish_message(message=message)
            assert await asyncio.wait_for(task, 2) == (channels[0], message)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await stream.aclose()
        await listener.aclose()
        await publisher.aclose()


@pytest.mark.asyncio
async def test_delayed_task_can_be_inspected_and_canceled(broker_backend, async_brokers, channels):
    name, config = broker_backend
    config = {
        'version': 2,
        'brokers': {name: {**config, 'channels': [channels[0]], 'default_publish_channel': channels[0]}},
        'producers': {'ControlProducer': {}},
        'service': {'main_kwargs': {'node_id': 'contract-node'}},
    }
    control = get_control_from_settings(settings=DispatcherSettings(config))
    publisher = async_brokers(default_publish_channel=channels[0])
    message = json.dumps({'task': 'lambda: "never run"', 'uuid': 'delayed-contract-task', 'delay': 30})

    async with adispatcher_service(config) as dispatcher:
        await publisher.apublish_message(message=message)

        async def wait_until_delayed():
            while True:
                replies = await control.acontrol_with_reply('running', timeout=2)
                if replies and 'delayed-0' in replies[0]:
                    return replies[0]['delayed-0']
                await asyncio.sleep(0.02)

        task = await asyncio.wait_for(wait_until_delayed(), 5)
        assert task['uuid'] == 'delayed-contract-task'
        canceled = await control.acontrol_with_reply('cancel', data={'uuid': task['uuid']}, timeout=2)
        assert canceled[0]['delayed-0']['uuid'] == task['uuid']
        remaining = await control.acontrol_with_reply('running', timeout=2)
        assert remaining == [{'node_id': 'contract-node'}]
        assert dispatcher.pool.finished_count == 0
