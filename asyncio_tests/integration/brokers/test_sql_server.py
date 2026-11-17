import asyncio
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from dispatcherd.brokers.sql_server import Broker


def _sql_server_config():
    config_json = os.environ.get("DISPATCHERD_SQLSERVER_TEST_CONFIG")
    if not config_json:
        pytest.skip("DISPATCHERD_SQLSERVER_TEST_CONFIG is not configured")
    config = json.loads(config_json)
    if not isinstance(config, dict):
        pytest.fail("DISPATCHERD_SQLSERVER_TEST_CONFIG must contain a JSON object")
    return config


@pytest.mark.asyncio
async def test_sql_server_broker_publishes_and_receives_large_payload():
    config = _sql_server_config()
    channel = f"dispatcherd_test_{uuid.uuid4().hex}"
    payload = "metrics ☃ " * 1000
    publisher = Broker(config=config, channels=(channel,), poll_interval_seconds=0.02)
    consumer = Broker(config=config, channels=(channel,), poll_interval_seconds=0.02)

    try:
        await publisher.apublish_message(channel=channel, message=payload)
        listener = consumer.aprocess_notify()
        assert await asyncio.wait_for(anext(listener), timeout=10) == (channel, payload)
        await listener.aclose()
    finally:
        await publisher.aclose()
        await consumer.aclose()


def test_sql_server_broker_two_consumers_claim_a_message_once():
    config = _sql_server_config()
    channel = f"dispatcherd_test_{uuid.uuid4().hex}"
    publisher = Broker(config=config, channels=(channel,))
    consumers = [Broker(config=config, channels=(channel,), poll_interval_seconds=0.02) for _ in range(2)]
    publisher.publish_message(channel=channel, message="one delivery")
    barrier = threading.Barrier(3)

    def consume(broker):
        barrier.wait()
        return list(broker.process_notify(timeout=1, max_messages=1))

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(consume, broker) for broker in consumers]
            barrier.wait()
            results = [future.result(timeout=5) for future in futures]
    finally:
        publisher.close()
        for broker in consumers:
            broker.close()

    assert results.count([(channel, "one delivery")]) == 1
    assert results.count([]) == 1
