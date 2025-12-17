from dispatcherd.producers.brokered import BrokeredProducer
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects


class DummyBroker:
    def get_self_check_stats(self):
        return {'enabled': True, 'success_count': 1}


def test_brokered_producer_reports_recycle_and_self_check_stats():
    producer = BrokeredProducer(broker=DummyBroker(), shared=SharedAsyncObjects())

    producer.record_recycle()
    producer.record_recycle()

    status = producer.get_status_data()
    assert status['recycle_count'] == 2
    broker_stats = status['broker_self_check']
    assert broker_stats['enabled'] is True
    assert broker_stats['success_count'] == 1
    assert status['uptime_seconds'] >= 0
