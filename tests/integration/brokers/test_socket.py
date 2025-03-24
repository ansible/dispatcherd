import asyncio
import socket
import time
import threading

import pytest

from dispatcherd.brokers.socket import Broker


@pytest.fixture
def socket_path(tmp_path):
    return str(tmp_path / 'test_sock.sock')


@pytest.mark.asyncio
async def test_basic_receive(socket_path):
    server_broker = Broker(socket_path=socket_path)
    client_broker = Broker(socket_path=socket_path)

    server_is_ready = asyncio.Event()

    async def on_connect():
        server_is_ready.set()

    received = []
    async def save_local():
        async for client_id, msg in server_broker.aprocess_notify(connected_callback=on_connect):
            received.append((client_id, msg))

    asyncio.create_task(save_local())

    await asyncio.wait_for(server_is_ready.wait(), timeout=2)

    for msg in ('test1', 'test2'):
        await client_broker.apublish_message(message=msg)

    for i in range(20):
        if len(received) >= 2:
            break
        await asyncio.sleep(0.01)
    else:
        assert 'Failed to receive expected 2 messages'

    await server_broker.aclose()

    assert received == [(0, 'test1'), (1, 'test2')]


@pytest.mark.asyncio
async def test_synchronous_listen_timeout(socket_path):
    client_broker = Broker(socket_path=socket_path)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)

    def run_server():
        server.listen(1)
        conn, addr = server.accept()
        conn.recv(1024)
        print('got message, exiting thread')

    server_thread = threading.Thread(target=run_server)
    server_thread.start()

    start = time.monotonic()
    received = None
    with pytest.raises(TimeoutError):
        received = [msg for _, msg in client_broker.process_notify(timeout=0.01)]
    delta = time.monotonic() - start
    assert delta >= 0.01

    assert received is None
    server_thread.join()
    assert not server_thread.is_alive()  # should have exited after getting message
