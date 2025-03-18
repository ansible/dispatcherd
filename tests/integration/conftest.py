import pytest

from dispatcher.testing import dispatcher_service
from dispatcher.factories import get_publisher_from_settings
from dispatcher.config import DispatcherSettings

from tests.conftest import CONNECTION_STRING


BASIC_CONFIG = {
    "version": 2,
    "brokers": {
        "pg_notify": {
            "channels": ['test_channel', 'test_channel2', 'test_channel3'],
            "config": {'conninfo': CONNECTION_STRING},
            "sync_connection_factory": "dispatcher.brokers.pg_notify.connection_saver",
            "default_publish_channel": "test_channel"
        }
    }
}


@pytest.fixture
def pg_dispatcher():
    with dispatcher_service(BASIC_CONFIG, pool_events=('work_cleared',)) as comms:
        yield comms


@pytest.fixture()
def pg_broker():
    settings = DispatcherSettings(BASIC_CONFIG)
    return get_publisher_from_settings(settings=settings)
