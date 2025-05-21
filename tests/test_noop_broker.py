import asyncio
import pytest

from dispatcherd.brokers.noop import Broker


class StreamYields:
    def __init__(self):
        self.received = []

    async def run(self):
        broker = Broker()
        async for channel, message in broker.aprocess_notify():
            self.received.append((channel, message))


@pytest.mark.asyncio
async def test_noop_broker_apublish_message():
    """Test that apublish_message does nothing."""
    broker = Broker()
    await broker.apublish_message(channel="test", message="test message")
    # No assertion needed as we're just verifying it doesn't raise an exception


@pytest.mark.asyncio
async def test_noop_broker_aprocess_notify():
    """Test that aprocess_notify yields empty messages."""
    streamer = StreamYields()
    str_task = asyncio.create_task(streamer.run())
    await asyncio.sleep(0.01)
    str_task.cancel()
    try:
        await str_task
    except asyncio.CancelledError:
        pass
    assert len(streamer.received) == 0  # assert we got no messages


def test_noop_broker_publish_message():
    """Test that publish_message returns empty string."""
    broker = Broker()
    result = broker.publish_message(channel="test", message="test message")
    assert result == ''


def test_noop_broker_process_notify():
    """Test that process_notify yields no messages."""
    broker = Broker()
    messages = list(broker.process_notify())
    assert len(messages) == 0


@pytest.mark.asyncio
async def test_noop_broker_aclose():
    """Test that aclose does nothing."""
    broker = Broker()
    await broker.aclose()
    # No assertion needed as we're just verifying it doesn't raise an exception


def test_noop_broker_close():
    """Test that close does nothing."""
    broker = Broker()
    broker.close()
    # No assertion needed as we're just verifying it doesn't raise an exception


def test_noop_broker_verify_self_check():
    """Test that verify_self_check does nothing."""
    broker = Broker()
    broker.verify_self_check({"test": "message"})
    # No assertion needed as we're just verifying it doesn't raise an exception 