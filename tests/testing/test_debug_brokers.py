import asyncio

import pytest

from dispatcherd.brokers import get_broker
from dispatcherd.testing.brokers.error_only import Broker as ErrorOnlyBroker
from dispatcherd.testing.brokers.memory import Broker as MemoryBroker
from dispatcherd.testing.brokers.noop import Broker as NoOpBroker


@pytest.fixture(params=[NoOpBroker, ErrorOnlyBroker])
def broker_class(request):
    return request.param


@pytest.fixture
def broker(broker_class):
    if broker_class is ErrorOnlyBroker:
        return broker_class(error_message='Test error')
    return broker_class()


def test_publish_message_behaviour(broker, broker_class):
    """Synchronous publish succeeds for noop and raises for error-only."""
    if broker_class is ErrorOnlyBroker:
        with pytest.raises(RuntimeError, match='Test error'):
            broker.publish_message(message='sync-test')
    else:
        assert broker.publish_message(message='sync-test') == ''


@pytest.mark.asyncio
async def test_apublish_message_behaviour(broker, broker_class):
    """Async publish succeeds for noop and raises for error-only."""
    if broker_class is ErrorOnlyBroker:
        with pytest.raises(RuntimeError, match='Test error'):
            await broker.apublish_message(message='async-test')
    else:
        await broker.apublish_message(message='async-test')


@pytest.mark.asyncio
async def test_aprocess_notify_is_cancelable(broker):
    """The brokers never yield, so waiting on aprocess_notify should time out."""

    async def never_finishes():
        async for _ in broker.aprocess_notify():
            return True
        return False

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(never_finishes(), timeout=0.05)


def test_memory_broker_records_sync_messages():
    broker = MemoryBroker()
    broker.publish_message(channel='chan', message='payload')
    assert broker.published_messages == [{'channel': 'chan', 'origin': None, 'message': 'payload', 'is_async': False}]


@pytest.mark.asyncio
async def test_memory_broker_records_async_messages():
    broker = MemoryBroker()
    await broker.apublish_message(channel='chan', origin='origin', message='payload')
    assert broker.published_messages == [{'channel': 'chan', 'origin': 'origin', 'message': 'payload', 'is_async': True}]


def test_memory_broker_process_notify_respects_max_messages():
    broker = MemoryBroker()
    broker.queue_notification(origin='first', message='payload-1')
    broker.queue_notification(origin='second', message='payload-2')

    notifications = list(broker.process_notify(max_messages=1))
    assert notifications == [('first', 'payload-1')]

    notifications = list(broker.process_notify())
    assert notifications == [('second', 'payload-2')]


@pytest.mark.asyncio
async def test_memory_broker_aprocess_notify_yields_enqueued_notifications():
    broker = MemoryBroker()
    agen = broker.aprocess_notify()
    broker.queue_notification(origin='async', message='payload')

    result = await asyncio.wait_for(agen.__anext__(), timeout=0.1)
    assert result == ('async', 'payload')
    await agen.aclose()


def test_get_broker_accepts_dotted_import_path():
    broker = get_broker('dispatcherd.testing.brokers.memory', {})
    assert isinstance(broker, MemoryBroker)
