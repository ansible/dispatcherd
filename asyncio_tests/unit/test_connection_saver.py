import asyncio

import pytest

from dispatcherd.brokers.pg_notify import async_connection_saver, connection_save
from tests.unit import test_connection_saver as sync_connection_saver


@pytest.mark.asyncio
async def test_async_connection_saver_thread_safety(monkeypatch):
    """async_connection_saver should behave like the sync version across tasks."""
    sync_connection_saver.connection_create_count = 0
    DummyConnection = sync_connection_saver.DummyConnection

    async def dummy_acreate_connection(**config):
        sync_connection_saver.connection_create_count += 1
        return DummyConnection()

    monkeypatch.setattr("dispatcherd.brokers.pg_notify.acreate_connection", dummy_acreate_connection)
    connection_save._async_connection = None

    async def worker():
        return await async_connection_saver(foo="bar")

    results = await asyncio.gather(*[worker() for _ in range(10)])
    assert all(r is results[0] for r in results)
    assert sync_connection_saver.connection_create_count == 1
    await results[0].aclose()
    assert results[0].closed is True
