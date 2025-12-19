import json
import time
from typing import Generator

import pytest
from conftest import CONNECTION_STRING

from dispatcherd.config import DispatcherSettings
from dispatcherd.factories import get_control_from_settings, get_publisher_from_settings
from dispatcherd.protocols import Broker
from dispatcherd.testing.subprocess import CommunicationItems, dispatcher_service

BASIC_CONFIG = {
    "version": 2,
    "brokers": {
        "pg_notify": {
            "channels": ['test_channel', 'test_channel2', 'test_channel3'],
            "config": {'conninfo': CONNECTION_STRING},
            "sync_connection_factory": "dispatcherd.brokers.pg_notify.connection_saver",
            "default_publish_channel": "test_channel",
        }
    },
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


def test_pg_notify_large_control_reply(pg_dispatcher, pg_broker, pg_control, get_worker_data):
    """Ensure pg_notify can transport replies larger than the native payload limit."""
    big_value = 'x' * 9001
    msg = json.dumps(
        {
            'task': 'lambda *args: __import__("time").sleep(3.1415)',
            'uuid': 'big_payload_control',
            'args': [big_value],
        }
    )
    pg_broker.publish_message(message=msg)

    time.sleep(0.2)
    running_jobs = pg_control.control_with_reply('running', timeout=2)
    assert len(running_jobs) == 1
    # Confirm the serialized payload that traveled over pg_notify exceeded 8k characters.
    assert len(json.dumps(running_jobs[0])) > 8000

    running_job = get_worker_data(running_jobs)
    assert running_job['uuid'] == 'big_payload_control'
    assert running_job['args'] == [big_value]

    pg_control.control_with_reply('cancel', data={'uuid': 'big_payload_control'}, timeout=1)
    status = pg_dispatcher.q_out.get(timeout=2)
    assert status == 'work_cleared'
