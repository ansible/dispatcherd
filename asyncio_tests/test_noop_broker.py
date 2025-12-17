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


@pytest.mark.asyncio
async def test_noop_broker_aclose():
    """Test that aclose does nothing."""
    broker = Broker()
    await broker.aclose()
