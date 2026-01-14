import asyncio
import logging
from typing import AsyncIterator

import httpx
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
        "service": {"main_kwargs": {"node_id": "metrics-test-server"}, "metrics_kwargs": {"port": TEST_METRICS_PORT}},
    }


@pytest_asyncio.fixture
async def ametrics_dispatcher(metrics_config) -> AsyncIterator[DispatcherMain]:
    async with adispatcher_service(metrics_config) as dispatcher:
        assert dispatcher.metrics.port == TEST_METRICS_PORT  # sanity, that config took effect
        yield dispatcher


async def aget_metrics():
    async with httpx.AsyncClient() as client:
        # Ensure the path is /metrics, as CustomHttpServer serves metrics on this specific path
        response = await client.get(f"http://localhost:{TEST_METRICS_PORT}/metrics")
        return response


@pytest.mark.asyncio
async def test_get_metrics(ametrics_dispatcher):
    await ametrics_dispatcher.metrics.ready_event.wait()

    # Actual test and assertion
    get_task = asyncio.create_task(aget_metrics())
    resp = await get_task
    assert resp.status_code == 200
    # Verify the Content-Type header for Prometheus metrics
    expected_content_type = "text/plain; version=0.0.4; charset=utf-8"
    assert resp.headers.get("content-type") == expected_content_type
    assert "dispatcher_messages_received_total" in resp.text
    # Check for another metric to be more thorough, e.g., worker_count
    assert "dispatcher_worker_count" in resp.text


@pytest.mark.asyncio
async def test_metrics_invalid_utf8_returns_400(ametrics_dispatcher):
    """Invalid UTF-8 in the request line should trigger a 400 response and close connection cleanly."""
    await ametrics_dispatcher.metrics.ready_event.wait()

    reader, writer = await asyncio.open_connection("localhost", TEST_METRICS_PORT)
    response_bytes = b""
    try:
        bad_request = b"GE\xffT /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        writer.write(bad_request)
        await writer.drain()

        response_bytes = await asyncio.wait_for(reader.read(), timeout=2)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    assert response_bytes.startswith(b"HTTP/1.1 400 Bad Request")
    assert b"Bad Request" in response_bytes
