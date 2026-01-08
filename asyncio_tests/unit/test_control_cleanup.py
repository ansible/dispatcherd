import json

import pytest

from dispatcherd.control import Control
from dispatcherd.protocols import Broker


# Dummy broker implementation for testing cleanup.
class DummyBroker(Broker):
    def __init__(self):
        self.aclose_called = False
        self.close_called = False
        self.sent_message = None

    async def aprocess_notify(self, connected_callback=None):
        if connected_callback:
            await connected_callback()
        # Yield one valid reply message.
        yield ("dummy_channel", json.dumps({"result": "ok"}))

    async def apublish_message(self, channel=None, message=""):
        self.sent_message = message

    async def aclose(self):
        self.aclose_called = True

    def process_notify(self, connected_callback=None, timeout: float = 5.0, max_messages: int | None = 1):
        if connected_callback:
            connected_callback()
        # Yield one valid reply message.
        yield ("dummy_channel", json.dumps({"result": "ok"}))

    def publish_message(self, channel=None, message=""):
        self.sent_message = message

    def close(self):
        self.close_called = True


# Test for async control with reply cleanup
@pytest.mark.asyncio
async def test_acontrol_with_reply_resource_cleanup(monkeypatch):
    dummy_broker = DummyBroker()

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
        return dummy_broker

    monkeypatch.setattr("dispatcherd.control.get_broker", dummy_get_broker)

    control = Control(broker_name="dummy", broker_config={})
    result = await control.acontrol_with_reply(command="test_command", expected_replies=1, timeout=2, data={"key": "value"})
    assert result == [{"result": "ok"}]
    assert dummy_broker.aclose_called is True


# Test for async control (fire-and-forget) cleanup
@pytest.mark.asyncio
async def test_acontrol_resource_cleanup(monkeypatch):
    dummy_broker = DummyBroker()

    def dummy_get_broker(broker_name, broker_config, channels=None, **kwargs):
        return dummy_broker

    monkeypatch.setattr("dispatcherd.control.get_broker", dummy_get_broker)

    control = Control(broker_name="dummy", broker_config={})
    await control.acontrol(command="test_command", data={"foo": "bar"})
    # In acontrol, broker.aclose() should be called.
    assert dummy_broker.aclose_called is True
