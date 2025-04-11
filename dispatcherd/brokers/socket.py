import asyncio
import json
import logging
import os
import socket
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Union

from ..protocols import Broker as BrokerProtocol
from ..service.asyncio_tasks import named_wait

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

    def write(self, message: str, /) -> None:
        self.writer.write((message + '\n').encode())

    def queue_reply(self, reply: str, /) -> None:
        self.replies_to_send.append(reply)

    async def send_replies(self) -> None:
        for reply in self.replies_to_send.copy():
            logger.info(f'Sending reply to client_id={self.client_id} len={len(reply)}')
            self.write(reply)
        else:
            logger.info(f'No replies to send to client_id={self.client_id}')
        await self.writer.drain()
        self.replies_to_send = []


def extract_json(message: str) -> Iterator[str]:
    """With message that may be an incomplete JSON string, yield JSON-complete strings and leftover"""
    decoder = json.JSONDecoder()
    pos = 0
    length = len(message)
    while pos < length:
        try:
            _, index = decoder.raw_decode(message, pos)
            json_msg = message[pos:index]
            yield json_msg
            pos = index
        except json.JSONDecodeError:
            break


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

    def __str__(self) -> str:
        return f'socket-broker-{self.socket_path}'

    async def _add_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = Client(self.client_ct, reader, writer)
        self.clients[self.client_ct] = client
        self.client_ct += 1
        current_task = asyncio.current_task()
        if current_task is not None:
            current_task.set_name(f'socket_client_task_{client.client_id}')
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
                await asyncio.wait_for(named_wait(client.yield_clear, f'internal_wait_for_client_{client.client_id}'), timeout=2)
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
        if isinstance(origin, int) and origin >= 0:
            client = self.clients.get(int(origin))
            if client:
                if client.listen_loop_active:
                    logger.info(f'Queued message len={len(message)} for client_id={origin}')
                    client.queue_reply(message)
                else:
                    logger.warning(f'Not currently listening to client_id={origin}, attempting reply len={len(message)}, but might be dropped')
                    client.write(message)
                    await client.writer.drain()
            else:
                logger.error(f'Client_id={origin} is not currently connected')
        else:
            # Acting as a client in this case, mostly for tests
            logger.info(f'Publishing async socket message len={len(message)} with new connection')
            writer = None
            try:
                _, writer = await asyncio.open_unix_connection(self.socket_path)
                writer.write((message + '\n').encode())
                await writer.drain()
            finally:
                if writer:
                    writer.close()
                    await writer.wait_closed()

    def process_notify(
        self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1
    ) -> Iterator[tuple[Union[int, str], str]]:
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                self.sock = sock
                sock.settimeout(timeout)
                sock.connect(self.socket_path)

                if connected_callback:
                    connected_callback()

                received_ct = 0
                buffer = ''
                while True:
                    response = sock.recv(1024).decode().strip()

                    current_message = buffer + response
                    yielded_chars = 0
                    for complete_msg in extract_json(current_message):
                        received_ct += 1
                        yield (0, complete_msg)
                        if received_ct >= max_messages:
                            return
                        yielded_chars += len(complete_msg)
                    else:
                        buffer = current_message[yielded_chars:]
                        logger.info(f'Received incomplete message len={len(buffer)}, adding to buffer')

        finally:
            self.sock = None

    def _publish_from_sock(self, sock: socket.socket, message: str) -> None:
        sock.sendall((message + "\n").encode())

    def publish_message(self, channel: Optional[str] = None, message: Optional[str] = None) -> None:
        assert isinstance(message, str)
        if self.sock:
            logger.info(f'Publishing socket message len={len(message)} via existing connection')
            self._publish_from_sock(self.sock, message)
        else:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.connect(self.socket_path)
                logger.info(f'Publishing socket message len={len(message)} over new connection')
                self._publish_from_sock(sock, message)
