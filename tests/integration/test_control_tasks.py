import pytest
import json
from typing import Generator
import time

from dispatcherd.testing.subprocess import dispatcher_service, CommunicationItems
from dispatcherd.factories import get_publisher_from_settings, get_control_from_settings
from dispatcherd.config import DispatcherSettings
from dispatcherd.protocols import Broker

from tests.conftest import CONNECTION_STRING


BASIC_CONFIG = {
    "version": 2,
    "brokers": {
        "pg_notify": {
            "channels": ['test_channel', 'test_channel2', 'test_channel3'],
            "config": {'conninfo': CONNECTION_STRING},
            "sync_connection_factory": "dispatcherd.brokers.pg_notify.connection_saver",
            "default_publish_channel": "test_channel"
        }
    }
}


@pytest.fixture
def pg_dispatcher(scope='module') -> Generator[CommunicationItems, None, None]:
    with dispatcher_service(BASIC_CONFIG, pool_events=('work_cleared',)) as comms:
        yield comms


@pytest.fixture(scope='module')
def pg_broker() -> Generator[Broker, None, None]:
    settings = DispatcherSettings(BASIC_CONFIG)
    return get_publisher_from_settings(settings=settings)


@pytest.fixture(scope='module')
def pg_control():
    settings = DispatcherSettings(BASIC_CONFIG)
    return get_control_from_settings(settings=settings)


def test_run_lambda_function(pg_dispatcher, pg_broker):
    pg_broker.publish_message(message='lambda: "This worked!"')
    message = pg_dispatcher.q_out.get(timeout=1)
    assert message == 'work_cleared'


def test_get_running_jobs(pg_dispatcher, pg_broker, pg_control, get_worker_data):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_me'})

    pg_broker.publish_message(message=msg)

    running_jobs = pg_control.control_with_reply('running', timeout=1)

    running_job = get_worker_data(running_jobs)

    assert running_job['uuid'] == 'find_me'


def test_cancel_task(pg_dispatcher, pg_broker, pg_control, get_worker_data):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    pg_broker.publish_message(message=msg)

    time.sleep(0.2)
    canceled_jobs = pg_control.control_with_reply('cancel', data={'uuid': 'foobar'}, timeout=1)
    canceled_message = get_worker_data(canceled_jobs)
    assert canceled_message['uuid'] == 'foobar'

    start = time.time()
    status = pg_dispatcher.q_out.get(timeout=1)
    assert status == 'work_cleared'
    delta = time.time() - start
    assert delta < 1.0  # less than sleep in test
