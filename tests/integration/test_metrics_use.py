import asyncio
import logging
import httpx
from typing import AsyncIterator

import pytest
import pytest_asyncio

from dispatcherd.protocols import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service

logger = logging.getLogger(__name__)


TEST_METRICS_PORT = 18080


@pytest.fixture(scope='session')
def metrics_config():
    return {
        "version": 2,
        "brokers": {},
        "service": {"main_kwargs": {"node_id": "metrics-test-server"}, "metrics_kwargs": {"log_level": "debug", "port": TEST_METRICS_PORT}},
    }


@pytest_asyncio.fixture
async def ametrics_dispatcher(metrics_config) -> AsyncIterator[DispatcherMain]:
    async with adispatcher_service(metrics_config) as dispatcher:
        yield dispatcher


async def aget_metrics():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://localhost:{TEST_METRICS_PORT}")
        return response


@pytest.mark.asyncio
async def test_get_metrics(ametrics_dispatcher):
    assert ametrics_dispatcher.metrics.port == TEST_METRICS_PORT  # sanity, that config took effect

    # Metrics server task starts from the main method
    main_task = asyncio.create_task(ametrics_dispatcher.main_as_task())

    # Actual test and assertion
    get_task = asyncio.create_task(aget_metrics())
    resp = await get_task
    assert resp.status_code == 200
    assert "dispatcher_messages_received_total" in resp.text

    # Normally handled by fixture, we made a main loop task, so take care of our own task
    await ametrics_dispatcher.shutdown()
    await main_task
