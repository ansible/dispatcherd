import json
import time
from typing import Generator

import pytest

from dispatcherd.config import DispatcherSettings
from dispatcherd.factories import get_control_from_settings, get_publisher_from_settings
from dispatcherd.protocols import Broker
from dispatcherd.testing.subprocess import CommunicationItems, dispatcher_service


@pytest.fixture
def db_service(basic_config) -> Generator[CommunicationItems, None, None]:
    # deliberately function-scoped: each test gets a fresh service so lingering
    # tasks (e.g. find_me's 3.14s sleep) cannot block work_cleared for the next test
    with dispatcher_service(basic_config, pool_events=('work_cleared',)) as comms:
        yield comms


@pytest.fixture(scope='module')
def db_publisher(basic_config) -> Generator[Broker, None, None]:
    settings = DispatcherSettings(basic_config)
    publisher = get_publisher_from_settings(settings=settings)
    yield publisher
    publisher.close()


@pytest.fixture(scope='module')
def db_control(basic_config):
    settings = DispatcherSettings(basic_config)
    return get_control_from_settings(settings=settings)


def test_run_lambda_function(db_service, db_publisher):
    db_publisher.publish_message(message='lambda: "This worked!"')
    message = db_service.q_out.get(timeout=1)
    assert message == 'work_cleared'


def test_get_running_jobs(db_service, db_publisher, db_control, get_worker_data):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_me'})

    db_publisher.publish_message(message=msg)

    running_jobs = db_control.control_with_reply('running', timeout=1)

    running_job = get_worker_data(running_jobs)

    assert running_job['uuid'] == 'find_me'


def test_cancel_task(db_service, db_publisher, db_control, get_worker_data):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    db_publisher.publish_message(message=msg)

    time.sleep(0.2)
    canceled_jobs = db_control.control_with_reply('cancel', data={'uuid': 'foobar'}, timeout=1)
    canceled_message = get_worker_data(canceled_jobs)
    assert canceled_message['uuid'] == 'foobar'

    start = time.time()
    status = db_service.q_out.get(timeout=1)
    assert status == 'work_cleared'
    delta = time.time() - start
    assert delta < 1.0  # less than sleep in test


def test_large_control_reply(db_service, db_publisher, db_control, get_worker_data):
    """Ensure the broker can transport replies larger than the native payload limit."""
    big_value = 'x' * 9001
    msg = json.dumps(
        {
            'task': 'lambda *args: __import__("time").sleep(3.1415)',
            'uuid': 'big_payload_control',
            'args': [big_value],
        }
    )
    db_publisher.publish_message(message=msg)

    time.sleep(0.2)
    running_jobs = db_control.control_with_reply('running', timeout=2)
    assert len(running_jobs) == 1
    # Confirm the serialized payload that traveled over the broker exceeded 8k characters.
    assert len(json.dumps(running_jobs[0])) > 8000

    running_job = get_worker_data(running_jobs)
    assert running_job['uuid'] == 'big_payload_control'
    assert running_job['args'] == [big_value]

    db_control.control_with_reply('cancel', data={'uuid': 'big_payload_control'}, timeout=1)
    status = db_service.q_out.get(timeout=2)
    assert status == 'work_cleared'
