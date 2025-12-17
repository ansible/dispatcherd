from dispatcherd.brokers.noop import Broker


def test_noop_broker_publish_message():
    """publish_message returns an empty string but does not raise."""
    broker = Broker()
    result = broker.publish_message(channel="test", message="test message")
    assert result == ''


def test_noop_broker_process_notify():
    """process_notify yields no messages for the noop broker."""
    broker = Broker()
    messages = list(broker.process_notify())
    assert len(messages) == 0


def test_noop_broker_close():
    """close should be a no-op."""
    broker = Broker()
    broker.close()


def test_noop_broker_verify_self_check():
    """verify_self_check should accept arbitrary dictionaries."""
    broker = Broker()
    broker.verify_self_check({"test": "message"})
