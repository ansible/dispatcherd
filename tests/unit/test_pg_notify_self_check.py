import time

import pytest

from dispatcherd.brokers.pg_notify import Broker as PGNotifyBroker
from tests.unit.utils import DummyAsyncConnection, DummySyncConnection


def _build_broker():
    return PGNotifyBroker(
        sync_connection=DummySyncConnection(),
        async_connection=DummyAsyncConnection(),
        channels=('demo',),
        max_connection_idle_seconds=5,
        max_self_check_message_age_seconds=2,
    )


def _self_check_message(broker: PGNotifyBroker) -> dict:
    return {'task': f'lambda: "{broker.broker_id}"'}


def test_self_check_stats_success_updates_average_and_max():
    broker = _build_broker()
    broker.last_self_check_message_time = time.monotonic() - 0.5

    broker.verify_self_check(_self_check_message(broker))

    stats = broker.get_self_check_stats()
    assert stats['enabled'] is True
    assert stats['success_count'] == 1
    assert stats['average_time_seconds'] == pytest.approx(0.5, rel=0.2)
    assert stats['max_time_seconds'] == pytest.approx(0.5, rel=0.2)


def test_self_check_failure_does_not_update_success_stats():
    broker = _build_broker()
    broker.last_self_check_message_time = time.monotonic() - 3

    with pytest.raises(RuntimeError):
        broker.verify_self_check(_self_check_message(broker))

    stats = broker.get_self_check_stats()
    assert stats['success_count'] == 0
    assert stats['average_time_seconds'] is None
    assert stats['max_time_seconds'] is None
