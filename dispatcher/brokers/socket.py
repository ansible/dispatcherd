import asyncio
import os
import socket
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional


class Broker:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.server = None  # For synchronous operations
        self.aserver = None  # For asynchronous operations
        self.client_socket = None  # For synchronous client
        self.client_reader = None  # For async client
        self.client_writer = None  # For async client

    def connect(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(self.socket_path)
        self.server.listen()
        self.server.settimeout(5.0)

    async def aconnect(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        self.aserver = await asyncio.start_unix_server(self._handle_connection, self.socket_path)

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.client_reader = reader
        self.client_writer = writer

    async def aprocess_notify(self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None) -> AsyncGenerator[tuple[str, str], None]:
        if not self.aserver:
            await self.aconnect()

        if connected_callback:
            await connected_callback()

        while True:
            try:
                line = await self.client_reader.readline()
                if not line:
                    break
                yield "unix_socket", line.decode().strip()
            except (asyncio.CancelledError, Exception):
                break

    async def apublish_message(self, channel: Optional[str] = None, message: str = "") -> None:
        if self.client_writer:
            self.client_writer.write((message + "\n").encode())
            await self.client_writer.drain()

    async def aclose(self) -> None:
        if self.client_writer:
            self.client_writer.close()
            await self.client_writer.wait_closed()
        if self.aserver:
            self.aserver.close()
            await self.aserver.wait_closed()
            self.aserver = None
            os.remove(self.socket_path)

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]:
        if not self.server:
            self.connect()

        self.client_socket, _ = self.server.accept()
        self.client_socket.settimeout(timeout)

        if connected_callback:
            connected_callback()

        count = 0
        while count < max_messages:
            try:
                data = self.client_socket.recv(1024)
                if not data:
                    break
                lines = data.decode().splitlines()
                for line in lines:
                    yield "unix_socket", line.strip()
                    count += 1
                    if count >= max_messages:
                        break
            except socket.timeout:
                break

    def publish_message(self, channel=None, message=None):
        if not self.server:
            self.connect()

        if self.client_socket:
            self.client_socket.sendall((message + "\n").encode())

    def close(self):
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
        if self.server:
            self.server.close()
            self.server = None
            os.remove(self.socket_path)
