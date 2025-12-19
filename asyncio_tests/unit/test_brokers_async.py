import asyncio

import pytest

from dispatcherd.brokers.error_only import Broker as ErrorOnlyBroker
from dispatcherd.brokers.noop import Broker as NoOpBroker


@pytest.fixture(params=[NoOpBroker, ErrorOnlyBroker])
def broker_class(request):
    return request.param


@pytest.fixture
def broker(broker_class):
    if broker_class == ErrorOnlyBroker:
        return broker_class(error_message="Test error message")
    return broker_class()


@pytest.mark.asyncio
async def test_broker_apublish_message(broker, broker_class):
    """Test that apublish_message behaves correctly for each broker type."""
    if broker_class == ErrorOnlyBroker:
        with pytest.raises(RuntimeError, match="Test error message"):
            await broker.apublish_message(message="test message")
    else:
        await broker.apublish_message(message="test message")  # Should not raise


@pytest.mark.asyncio
async def test_broker_aprocess_notify(broker):
    """Test that aprocess_notify never yields messages and can be cancelled."""

    async def try_get_next():
        async for _ in broker.aprocess_notify():
            return True  # Should never happen
        return False

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(try_get_next(), timeout=0.2)
