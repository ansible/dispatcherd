import asyncio
import logging
import os
import socket
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Union

from ..protocols import Broker as BrokerProtocol

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, client_id: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.client_id = client_id
        self.reader = reader
        self.writer = writer
        self.listen_loop_active = False
        # This is needed for task management betewen the client tasks and the main aprocess_notify
        # if the client task starts listening, then we can not send replies
        # so this waits for the caller method to add replies to stack before continuing
        self.yield_clear = asyncio.Event()
        self.replies_to_send: list = []

    def write(self, message) -> None:
        self.writer.write((message + '\n').encode())

    def queue_reply(self, reply: str) -> None:
        self.replies_to_send.append(reply)

    async def send_replies(self):
        for reply in self.replies_to_send.copy():
            logger.info(f'Sending reply to client_id={self.client_id} len={len(reply)}')
            self.write(reply)
        else:
            logger.info(f'No replies to send to client_id={self.client_id}')
        await self.writer.drain()
        self.replies_to_send = []


class Broker(BrokerProtocol):
    """A Unix socket client for dispatcher as simple as possible

    Because we want to be as simple as possible we do not maintain persistent connections.
    So every control-and-reply command will connect and disconnect.

    Intended use is for dispatcherctl, so that we may bypass any flake related to pg_notify
    for debugging information.
    """

    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.client_ct = 0
        self.clients: dict[int, Client] = {}
        self.sock: Optional[socket.socket] = None  # for synchronous clients
        self.incoming_queue: asyncio.Queue = asyncio.Queue()

    async def _add_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = Client(self.client_ct, reader, writer)
        self.clients[self.client_ct] = client
        self.client_ct += 1
        logger.info(f'Socket client_id={client.client_id} is connected')

        try:
            client.listen_loop_active = True
            while True:
                line = await client.reader.readline()
                if not line:
                    break  # disconnect
                message = line.decode().strip()
                await self.incoming_queue.put((client.client_id, message))
                # Wait for caller to potentially fill a reply queue
                # this should realistically never take more than a trivial amount of time
                await asyncio.wait_for(client.yield_clear.wait(), timeout=2)
                client.yield_clear.clear()
                await client.send_replies()
        except asyncio.TimeoutError:
            logger.error(f'Unexpected asyncio task management bug for client_id={client.client_id}, exiting')
        except asyncio.CancelledError:
            logger.debug(f'Ack that reader task for client_id={client.client_id} has been canceled')
        except Exception:
            logger.exception(f'Exception from reader task for client_id={client.client_id}')
        finally:
            del self.clients[client.client_id]
            client.writer.close()
            await client.writer.wait_closed()
            logger.info(f'Socket client_id={client.client_id} is disconnected')

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[Union[int, str], str], None]:
        if os.path.exists(self.socket_path):
            logger.debug(f'Deleted pre-existing {self.socket_path}')
            os.remove(self.socket_path)

        aserver = None
        try:
            aserver = await asyncio.start_unix_server(self._add_client, self.socket_path)
            logger.info(f'Set up socket server on {self.socket_path}')

            if connected_callback:
                await connected_callback()

            while True:
                client_id, message = await self.incoming_queue.get()
                if (client_id == -1) and (message == 'stop'):
                    return  # internal exit signaling from aclose

                yield client_id, message
                # trigger reply messages if applicable
                client = self.clients.get(client_id)
                if client:
                    logger.info(f'Yield complete for client_id={client_id}')
                    client.yield_clear.set()

        except asyncio.CancelledError:
            logger.debug('Ack that general socket server task has been canceled')
        finally:
            if aserver:
                aserver.close()
                await aserver.wait_closed()

            for client in self.clients.values():
                client.writer.close()
                await client.writer.wait_closed()
            self.clients = {}

            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)

    async def aclose(self) -> None:
        """Send an internal message to the async generator, which will cause it to close the server"""
        await self.incoming_queue.put((-1, 'stop'))

    async def apublish_message(self, channel: Optional[str] = '', origin: Union[int, str, None] = None, message: str = "") -> None:
        logger.warning(f'apublish_message with socket {(channel, origin, len(message))}')
        if isinstance(origin, int) and origin >= 0:
            client = self.clients.get(int(origin))
            if client:
                if client.listen_loop_active:
                    logger.info(f'Queued message for client_id={client.client_id}')
                    client.queue_reply(message)
                else:
                    logger.warning(f'Not currently listening to client_id={client.client_id}, reply might be dropped')
                    client.write(message)
                    await client.writer.drain()
            else:
                logger.error(f'Client_id={origin} is not currently connected')

    def process_notify(
        self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1
    ) -> Iterator[tuple[Union[int, str], str]]:
        received_ct = 0
        buffer = ''
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                self.sock = sock
                sock.settimeout(timeout)
                sock.connect(self.socket_path)

                if connected_callback:
                    connected_callback()

                while True:
                    response = sock.recv(1024).decode().strip()

                    if not response:
                        logger.info(f'Received {received_ct} of {max_messages} in {timeout}, exiting receiving')
                        return

                    if response.endswith('}'):

                        response = buffer + response
                        buffer = ''
                        received_ct += 1
                        yield (0, response)
                        if received_ct >= max_messages:
                            return
                    else:
                        buffer += response
        finally:
            self.sock = None

    def _publish_from_sock(self, sock, message) -> None:
        sock.sendall((message + "\n").encode())

    def publish_message(self, channel=None, message=None) -> None:
        if self.sock:
            self._publish_from_sock(self.sock, message)
        else:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.connect(self.socket_path)
                self._publish_from_sock(sock, message)
