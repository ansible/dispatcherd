import pytest

from dispatcherd.brokers.noop import Broker as NoOpBroker
from dispatcherd.brokers.error_only import Broker as ErrorOnlyBroker


@pytest.fixture(params=[NoOpBroker, ErrorOnlyBroker])
def broker_class(request):
    return request.param


@pytest.fixture
def broker(broker_class):
    if broker_class == ErrorOnlyBroker:
        return broker_class(error_message="Test error message")
    return broker_class()


def test_broker_publish_message(broker, broker_class):
    """Test that publish_message behaves correctly for each broker type."""
    if broker_class == ErrorOnlyBroker:
        with pytest.raises(RuntimeError, match="Test error message"):
            broker.publish_message(message="test message")
    else:
        broker.publish_message(message="test message")  # Should not raise


def test_broker_process_notify(broker):
    """Test that process_notify yields no messages."""
    messages = list(broker.process_notify())
    assert len(messages) == 0
