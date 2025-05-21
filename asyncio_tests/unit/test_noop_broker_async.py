import pytest

from dispatcherd.brokers.noop import Broker


@pytest.mark.asyncio
async def test_noop_broker_apublish_message():
    """Test that apublish_message does nothing."""
    broker = Broker()
    await broker.apublish_message(channel="test", message="test message")
    # No assertion needed as we're just verifying it doesn't raise an exception


@pytest.mark.asyncio
async def test_noop_broker_aprocess_notify():
    """Test that aprocess_notify yields empty messages."""
    broker = Broker()
    async for channel, message in broker.aprocess_notify():
        assert channel == ''
        assert message == ''
        break  # Just test one iteration
