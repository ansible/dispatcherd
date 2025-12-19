import json

from dispatcherd.brokers.pg_notify import Broker as PGNotifyBroker
from dispatcherd.control import Control
from dispatcherd.protocols import Broker
from tests.unit.utils import DummyAsyncConnection, DummySyncConnection


class DummyBroker(Broker):
    """Simple broker stub to ensure cleanup paths close resources."""

    def __init__(self):
        self.close_called = False
        self.sent_message = None

    async def aprocess_notify(self, connected_callback=None):
        if connected_callback:
            await connected_callback()
        yield ("dummy_channel", json.dumps({"result": "ok"}))

    async def apublish_message(self, channel=None, message=""):
        self.sent_message = message

    async def aclose(self):
        return

    def process_notify(self, connected_callback=None, timeout: float = 5.0, max_messages: int | None = 1):
        if connected_callback:
            connected_callback()
        yield ("dummy_channel", json.dumps({"result": "ok"}))

    def publish_message(self, channel=None, message=""):
        self.sent_message = message

    def close(self):
        self.close_called = True


def test_control_with_reply_resource_cleanup(monkeypatch):
    """control_with_reply should close the broker when finished."""
    dummy_broker = DummyBroker()

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
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
