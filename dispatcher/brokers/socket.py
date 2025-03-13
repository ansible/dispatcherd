import asyncio
import logging
import os
import socket
import threading
import time
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, client_id: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.client_id = client_id
        self.reader = reader
        self.writer = writer


class Broker:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.aserver = None
        self.client_ct = 0
        self.clients = {}
        self.sock = None  # for synchronous clients
        self.incoming_queue = asyncio.Queue()

    async def aconnect(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        self.aserver = await asyncio.start_unix_server(self._add_client, self.socket_path)

    async def _add_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = Client(self.client_ct, reader, writer)
        self.clients[self.client_ct] = client
        self.client_ct += 1

        try:
            while True:
                line = await client.reader.readline()
                if not line:
                    break  # disconnect
                message = line.decode().strip()
                await self.incoming_queue.put((client.client_id, message))
        except asyncio.CancelledError:
            logger.debug(f'Ack that reader task for client_id={client.client_id} has been canceled')
        except Exception:
            logger.exception(f'Exception from reader task for client_id={client.client_id}')
        finally:
            del self.clients[client.client_id]
            client.writer.close()
            await client.writer.wait_closed()
            logger.info(f'Client_id={client.client_id} has disconnected')

    async def aprocess_notify(self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None) -> AsyncGenerator[tuple[str, str], None]:
        if not self.aserver:
            await self.aconnect()

        if connected_callback:
            await connected_callback()

        while True:
            client_id, message = await self.incoming_queue.get()
            yield client_id, message

    async def apublish_message(self, channel: Optional[str] = '', origin: Optional[int] = None, message: str = "") -> None:
        client = self.clients.get(origin)
        if client:
            client.writer((message + '\n').encode())
            await client.writer.drain()
        else:
            logger.error(f'Client_id={origin} is not currently connected')

    async def aclose(self) -> None:
        if self.aserver:
            self.aserver.close()
            await self.aserver.wait_closed()
        self.aserver = None

        for client in self.clients.values():
            client.writer.close()
            await client.writer.wait_closed()
        self.clients = {}

        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

    def _enforce_timeout(self, timeout, stop_event, sock, lock):
        """Used for enforce timeout in a thread"""
        time.sleep(timeout)
        with lock:
            if stop_event.is_set():
                return
            sock.close()

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]:
        received_ct = 0
        lock = threading.Lock()
        stop_event = threading.Event()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                self.sock = sock
                sock.connect(self.socket_path)

                if connected_callback:
                    connected_callback()

                timeout_thread = threading.Thread(thread=self._enforce_timeout, daemon=True, args=(timeout, stop_event, self.sock, lock))
                timeout_thread.start()

                while True:
                    response = sock.recv(1024).decode().strip()

                    if not response:
                        logger.info(f'Received {received_ct} of {max_messages} in {timeout}, exiting receiving')
                        return

                    received_ct += 1
                    yield (0, response)
                    if received_ct >= max_messages:
                        with self.lock:
                            stop_event.set()
                        return
        finally:
            self.sock = None

    def _publish_from_sock(self, sock, message):
        sock.sendall((message + "\n").encode())

    def publish_message(self, channel=None, message=None):
        if not self.sock:
            self._publish_from_sock(self.sock, message)

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(self.socket_path)
            self._publish_from_sock(sock, message)

    def close(self):
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
        if self.server:
            self.server.close()
            self.server = None
            os.remove(self.socket_path)
