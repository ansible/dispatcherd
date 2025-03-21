import asyncio
import threading

import pytest

from dispatcherd.brokers.pg_notify import async_connection_saver, connection_save, connection_saver


# Define a dummy connection object that supports both sync and async close methods.
class DummyConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    async def aclose(self):
        self.close()


connection_create_count = 0


def dummy_create_connection(**config):
    global connection_create_count
    connection_create_count += 1
    return DummyConnection()


@pytest.fixture(autouse=True)
def reset_sync(monkeypatch):
    global connection_create_count
    connection_create_count = 0
    monkeypatch.setattr("dispatcherd.brokers.pg_notify.create_connection", dummy_create_connection)
    connection_save._connection = None


def test_connection_saver_thread_safety():
    results = []

    def worker():
        res = connection_saver(foo="bar")
        results.append(res)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Ensure all threads got the same connection object.
    assert all(r is results[0] for r in results)
    # Ensure only one connection was created.
    assert connection_create_count == 1
    # Check that the connection supports close() properly.
    results[0].close()
    assert results[0].closed is True


@pytest.mark.asyncio
async def test_async_connection_saver_thread_safety(monkeypatch):
    global connection_create_count
    connection_create_count = 0

    async def dummy_acreate_connection(**config):
        global connection_create_count
        connection_create_count += 1
        return DummyConnection()

    monkeypatch.setattr("dispatcherd.brokers.pg_notify.acreate_connection", dummy_acreate_connection)
    connection_save._async_connection = None

    async def worker():
        return await async_connection_saver(foo="bar")

    results = await asyncio.gather(*[worker() for _ in range(10)])
    # Ensure all tasks returned the same connection object.
    assert all(r is results[0] for r in results)
    # Ensure only one async connection was created.
    assert connection_create_count == 1
    await results[0].aclose()
    assert results[0].closed is True
