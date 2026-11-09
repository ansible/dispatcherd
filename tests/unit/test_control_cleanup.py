import json

import pytest

from dispatcherd.brokers.pg_notify import Broker as PGNotifyBroker
from dispatcherd.control import Control
from dispatcherd.protocols import Broker
from tests.unit.utils import DummyAsyncConnection, DummySyncConnection


class DummyBroker(Broker):
    """Simple broker stub to ensure cleanup paths close resources."""

    def __init__(self, channel="dummy_channel"):
        self.close_called = False
        self.sent_message = None
        self.channel = channel

    async def aprocess_notify(self, connected_callback=None):
        if connected_callback:
            await connected_callback()
        yield (self.channel, json.dumps({"result": "ok"}))

    async def apublish_message(self, channel=None, message=""):
        self.sent_message = message

    async def aclose(self):
        self.close_called = True

    def process_notify(self, connected_callback=None, timeout: float = 5.0, max_messages: int | None = 1):
        if connected_callback:
            connected_callback()
        yield (self.channel, json.dumps({"result": "ok"}))

    def publish_message(self, channel=None, message=""):
        self.sent_message = message

    def close(self):
        self.close_called = True


def test_control_with_reply_resource_cleanup(monkeypatch):
    """control_with_reply should close the broker when finished."""
    dummy_broker = DummyBroker()

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
        if channels:
            dummy_broker.channel = channels[0]
        return dummy_broker

    monkeypatch.setattr("dispatcherd.control.get_broker", dummy_get_broker)

    control = Control(broker_name="dummy", broker_config={}, queue="test_queue")
    result = control.control_with_reply(command="test_command", expected_replies=1, timeout=2, data={"foo": "bar"})
    assert result == [{"result": "ok"}]
    assert dummy_broker.close_called is True


def test_control_resource_cleanup(monkeypatch):
    """control() should close the broker even when fire-and-forget."""
    dummy_broker = DummyBroker()

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
        return dummy_broker

    monkeypatch.setattr("dispatcherd.control.get_broker", dummy_get_broker)

    control = Control(broker_name="dummy", broker_config={}, queue="test_queue")
    control.control(command="test_command", data={"foo": "bar"})
    assert dummy_broker.close_called is True


def test_control_with_reply_discards_stale_notifications(monkeypatch):
    """control_with_reply should discard replies on mismatched channels."""
    dummy_broker = DummyBroker(channel="stale_reply_channel")

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
        return dummy_broker

    monkeypatch.setattr("dispatcherd.control.get_broker", dummy_get_broker)

    control = Control(broker_name="dummy", broker_config={}, queue="test_queue")
    result = control.control_with_reply(command="test_command", expected_replies=1, timeout=1, data={"foo": "bar"})
    assert result == []
    assert dummy_broker.close_called is True


def test_pg_notify_control_overrides_disable_self_checks():
    control = Control(broker_name="pg_notify", broker_config={})
    overrides = control._control_broker_overrides()
    broker = PGNotifyBroker(
        sync_connection=DummySyncConnection(),
        async_connection=DummyAsyncConnection(),
        channels=('reply_channel',),
        **overrides,
    )

    assert broker.max_connection_idle_seconds is None
    assert broker.max_self_check_message_age_seconds is None
    assert broker.self_check_channel is None
    assert all('self_check' not in channel for channel in broker.channels)
    stats = broker.get_self_check_stats()
    assert stats['enabled'] is False
    assert stats['success_count'] == 0
    assert stats['average_time_seconds'] is None
    assert stats['max_time_seconds'] is None


@pytest.mark.asyncio
async def test_async_control_keeps_channel_fallback(monkeypatch):
    broker = DummyBroker()

    def create(broker_name, broker_config, channels=None, **kwargs):
        broker.channel = channels[0]
        return broker

    monkeypatch.setattr('dispatcherd.control.get_broker', create)
    control = Control('dummy', {}, queue='requests')
    assert await control.acontrol_with_reply('alive') == [{'result': 'ok'}]
    assert json.loads(broker.sent_message)['reply_to'] == broker.channel
    assert broker.close_called


@pytest.mark.parametrize('async_client', [False, True])
@pytest.mark.asyncio
async def test_optional_reply_endpoint_prepared_before_send(monkeypatch, async_client):
    class DirectBroker(DummyBroker):
        def open_reply_endpoint(self, timeout):
            assert self.sent_message is None
            self.channel = 'opaque-return-address'
            return self.channel

        async def aopen_reply_endpoint(self, timeout):
            return self.open_reply_endpoint(timeout)

    broker = DirectBroker()
    monkeypatch.setattr('dispatcherd.control.get_broker', lambda *args, **kwargs: broker)
    control = Control('direct', {}, queue='requests')
    if async_client:
        result = await control.acontrol_with_reply('alive')
    else:
        result = control.control_with_reply('alive')
    assert result == [{'result': 'ok'}]
    assert json.loads(broker.sent_message)['reply_to'] == 'opaque-return-address'
    assert broker.close_called


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['prepare', 'receive'])
@pytest.mark.parametrize('outcome', ['timeout', 'cancel', 'error'])
async def test_async_reply_cleanup_on_interrupted_operation(monkeypatch, stage, outcome):
    import asyncio

    entered = asyncio.Event()

    async def interrupt():
        entered.set()
        if outcome == 'error':
            raise RuntimeError('broker failed')
        await asyncio.Event().wait()

    class InterruptedBroker(DummyBroker):
        async def aopen_reply_endpoint(self, timeout):
            if stage == 'prepare':
                await interrupt()
            return self.channel

        async def aprocess_notify(self, connected_callback=None):
            await connected_callback()
            yield (self.channel, json.dumps({'result': 'first'}))
            await interrupt()

    broker = InterruptedBroker()
    monkeypatch.setattr('dispatcherd.control.get_broker', lambda *args, **kwargs: broker)
    control = Control('dummy', {}, queue='requests')
    task = asyncio.create_task(control.acontrol_with_reply('alive', expected_replies=2, timeout=0.05 if outcome == 'timeout' else 5))
    await asyncio.wait_for(entered.wait(), 1)
    if outcome == 'cancel':
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif outcome == 'error':
        with pytest.raises(RuntimeError, match='broker failed'):
            await task
    else:
        assert await task == ([] if stage == 'prepare' else [{'result': 'first'}])
    assert broker.close_called
    assert (broker.sent_message is None) == (stage == 'prepare')


@pytest.mark.parametrize('stage', ['prepare', 'receive'])
@pytest.mark.parametrize('exception', [TimeoutError, ConnectionError])
def test_sync_reply_timeout_and_cleanup(monkeypatch, stage, exception):
    class FailingBroker(DummyBroker):
        def open_reply_endpoint(self, timeout):
            if stage == 'prepare':
                raise exception('prepare failed')
            return self.channel

        def process_notify(self, connected_callback=None, **kwargs):
            connected_callback()
            yield self.channel, json.dumps({'result': 'first'})
            raise exception('receive failed')

    broker = FailingBroker()
    monkeypatch.setattr('dispatcherd.control.get_broker', lambda *args, **kwargs: broker)
    control = Control('dummy', {}, queue='requests')
    if exception is TimeoutError:
        assert control.control_with_reply('alive', expected_replies=2) == ([] if stage == 'prepare' else [{'result': 'first'}])
    else:
        with pytest.raises(ConnectionError):
            control.control_with_reply('alive', expected_replies=2)
    assert broker.close_called
