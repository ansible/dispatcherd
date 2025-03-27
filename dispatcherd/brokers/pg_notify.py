import json
import logging
import threading
import time
import uuid
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Union

import psycopg

from ..protocols import Broker as BrokerProtocol
from ..protocols import BrokerSelfCheckStatus
from ..utils import resolve_callable

logger = logging.getLogger(__name__)


"""This module exists under the theory that dispatcherd messaging should be swappable

to different message busses eventually.
That means that the main code should never import psycopg.
Thus, all psycopg-lib-specific actions must happen here.
"""


async def acreate_connection(**config) -> psycopg.AsyncConnection:  # type: ignore[no-untyped-def]
    "Create a new asyncio connection"
    connection = await psycopg.AsyncConnection.connect(**config)
    if not connection.autocommit:
        await connection.set_autocommit(True)
    return connection


def create_connection(**config) -> psycopg.Connection:  # type: ignore[no-untyped-def]
    connection = psycopg.Connection.connect(**config)
    if not connection.autocommit:
        connection.set_autocommit(True)
    return connection


class Broker(BrokerProtocol):
    NOTIFY_QUERY_TEMPLATE = 'SELECT pg_notify(%s, %s);'

    def __init__(
        self,
        config: Optional[dict] = None,
        async_connection_factory: Optional[str] = None,
        sync_connection_factory: Optional[str] = None,
        sync_connection: Optional[psycopg.Connection] = None,
        async_connection: Optional[psycopg.AsyncConnection] = None,
        channels: Union[tuple, list] = (),
        default_publish_channel: Optional[str] = None,
        max_connection_idle_seconds: Optional[int] = 30,
        max_self_check_message_age_seconds: Optional[int] = 2,
    ) -> None:
        """
        config - kwargs to psycopg connect classes, if creating connection this way
        (a)sync_connection_factory - importable path to callback for creating
          the psycopg connection object, the normal or synchronous version
          this will have the config passed as kwargs, if that is also given
        async_connection - directly pass the async connection object
        sync_connection - directly pass the async connection object
        channels - listening channels for the service and used for control-and-reply
        default_publish_channel - if not specified on task level or in the submission
          by default messages will be sent to this channel.
          this should be one of the listening channels for messages to be received.
        """
        if not (config or async_connection_factory or async_connection):
            raise RuntimeError('Must specify either config or async_connection_factory')

        if not (config or sync_connection_factory or sync_connection):
            raise RuntimeError('Must specify either config or sync_connection_factory')

        if max_connection_idle_seconds:
            if max_self_check_message_age_seconds is None:
                raise RuntimeError('max_self_check_message_age_seconds must be specified if health checks are enabled')
            if max_self_check_message_age_seconds > max_connection_idle_seconds:
                raise RuntimeError('max_self_check_message_age_seconds must be smaller than max_connection_idle_seconds')

        # Used to identify the broker in self check messages
        self.broker_id = f"broker_{str(uuid.uuid4()).replace('-', '_')}"

        self.max_connection_idle_seconds = max_connection_idle_seconds
        self.max_self_check_message_age_seconds = max_self_check_message_age_seconds

        self._async_connection_factory = async_connection_factory
        self._async_connection = async_connection
        # If a connection is created by the factory, we will not manage it (closing) here
        self.owns_async_connection = False

        self._sync_connection_factory = sync_connection_factory
        self._sync_connection = sync_connection
        self.owns_sync_connection = False

        if config:
            self._config: dict = config.copy()
        else:
            self._config = {}

        self.user_channels = channels
        # Generate a special channel for broker self checks
        self.self_check_channel = self.generate_self_check_channel_name()

        server_channels = list(channels)
        if self.self_check_channel not in server_channels:
            server_channels.append(self.self_check_channel)
        self.channels = server_channels

        self.default_publish_channel = default_publish_channel
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic()

        # If we are in the notification loop (receiving messages),
        # then we have to break out before sending messages
        # These variables track things so that we can exit, send, and re-enter
        self.notify_loop_active: bool = False
        self.notify_queue: list = []

    @classmethod
    def generate_self_check_channel_name(cls) -> str:
        return f"self_check_{str(uuid.uuid4()).replace('-', '_')}"

    def get_publish_channel(self, channel: Optional[str] = None) -> str:
        "Handle default for the publishing channel for calls to publish_message, shared sync and async"
        if channel is not None:
            return channel
        elif self.default_publish_channel is not None:
            return self.default_publish_channel
        elif len(self.user_channels) == 1:
            # de-facto default channel, because there is only 1
            return self.channels[0]

        raise ValueError('Could not determine a channel to use publish to from settings or PGNotify config')

    def __str__(self) -> str:
        return 'pg_notify-broker'

    # --- asyncio connection methods ---

    async def aget_connection(self) -> psycopg.AsyncConnection:
        # Check if the cached async connection is either None or closed.
        if not self._async_connection or getattr(self._async_connection, "closed", 0) != 0:
            if self._async_connection_factory:
                factory = resolve_callable(self._async_connection_factory)
                if not factory:
                    raise RuntimeError(f'Could not import async connection factory {self._async_connection_factory}')
                connection = await factory(**self._config)
            elif self._config:
                self.owns_async_connection = True
                connection = await acreate_connection(**self._config)
            else:
                raise RuntimeError('Could not construct async connection for lack of config or factory')
            self._async_connection = connection
        assert self._async_connection is not None
        return self._async_connection

    def get_listen_query(self, channel: str) -> psycopg.sql.Composed:
        """Returns SQL command for listening on pg_notify channel

        This uses the psycopg utilities which ensure correct escaping so SQL injection is not possible.
        Return value is a valid argument for cursor.execute()
        """
        return psycopg.sql.SQL("LISTEN {};").format(psycopg.sql.Identifier(channel))

    def get_unlisten_query(self) -> psycopg.sql.SQL:
        """Stops listening on all channels for current session, see pg_notify docs"""
        return psycopg.sql.SQL("UNLISTEN *;")

    async def initiate_self_check(self) -> None:
        if self.self_check_status == BrokerSelfCheckStatus.IN_PROGRESS:
            # another self-check message is in flight
            delta = time.monotonic() - self.last_self_check_message_time
            raise RuntimeError(f'self check message for broker {self.broker_id} did not arrive in {delta} seconds')

        await self.apublish_message(channel=self.self_check_channel, message=json.dumps({'self_check': True, 'task': f'lambda: "{self.broker_id}"'}))
        self.self_check_status = BrokerSelfCheckStatus.IN_PROGRESS
        self.last_self_check_message_time = time.monotonic()

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """Verify a received self check message: check if it was sent from the same node and
        is not outdated
        """
        if self.broker_id not in message['task']:
            # sent from a different node, ignore it
            logger.debug(f'Ignoring self-check message due to broker_id not matching, {message["task"]}!={self.broker_id}')
            return

        now = time.monotonic()
        assert self.last_self_check_message_time is not None
        delta_seconds = now - self.last_self_check_message_time

        # request/response cycle completed, reset the status back to idle
        self.self_check_status = BrokerSelfCheckStatus.IDLE

        assert self.max_self_check_message_age_seconds is not None
        if delta_seconds < self.max_self_check_message_age_seconds:
            logger.info(f'self check succeeded, message received after {round(delta_seconds, 2)} seconds, broker-id {self.broker_id}')
        else:
            raise RuntimeError(f'self check failed, message received after {round(delta_seconds, 2)} seconds, broker-id {self.broker_id}')

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:  # public
        connection = await self.aget_connection()
        async with connection.cursor() as cur:
            for channel in self.channels:
                await cur.execute(self.get_listen_query(channel))
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                await connected_callback()

            while True:
                logger.debug('Starting listening for pg_notify notifications')
                self.notify_loop_active = True
                async for notify in connection.notifies(timeout=self.max_connection_idle_seconds):
                    yield notify.channel, notify.payload
                    if self.notify_queue:
                        break
                else:
                    logger.info(
                        f'No message received since {self.max_connection_idle_seconds} seconds, starting self check to channel={self.self_check_channel}'
                    )
                    await self.initiate_self_check()

                self.notify_loop_active = False
                for reply_to, reply_message in self.notify_queue:
                    await self.apublish_message_from_cursor(cur, channel=reply_to, message=reply_message)
                self.notify_queue = []

    async def apublish_message_from_cursor(self, cursor: psycopg.AsyncCursor, channel: Optional[str] = None, message: str = '') -> None:
        """The inner logic of async message publishing where we already have a cursor"""
        await cursor.execute(self.NOTIFY_QUERY_TEMPLATE, (channel, message))

    async def apublish_message(self, channel: Optional[str] = None, origin: Union[str, int, None] = '', message: str = '') -> None:  # public
        """asyncio way to publish a message, used to send control in control-and-reply

        Not strictly necessary for the service itself if it sends replies in the workers,
        but this may change in the future.
        """
        if self.notify_loop_active:
            self.notify_queue.append((channel, message))
            return

        connection = await self.aget_connection()
        channel = self.get_publish_channel(channel)

        async with connection.cursor() as cur:
            await self.apublish_message_from_cursor(cur, channel=channel, message=message)

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    async def aclose(self) -> None:
        if self.owns_async_connection and self._async_connection:
            logger.info('Closing asynchronous psycopg connection')
            await self._async_connection.close()
            self._async_connection = None
            self.owns_async_connection = False

        # Reset any server-related vars from __init__
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic()
        self.notify_loop_active = False
        self.notify_queue = []

    # --- synchronous connection methods ---

    def get_connection(self) -> psycopg.Connection:
        # Check if the cached connection is either None or closed.
        if not self._sync_connection or getattr(self._sync_connection, "closed", 0) != 0:
            if self._sync_connection_factory:
                factory = resolve_callable(self._sync_connection_factory)
                if not factory:
                    raise RuntimeError(f'Could not import connection factory {self._sync_connection_factory}')
                connection = factory(**self._config)
            elif self._config:
                self.owns_sync_connection = True
                connection = create_connection(**self._config)
            else:
                raise RuntimeError('Could not construct connection for lack of config or factory')
            self._sync_connection = connection
        assert self._sync_connection is not None
        return self._sync_connection

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]:
        """Blocking method that listens for messages on subscribed pg_notify channels until timeout

        This has two different exit conditions:
        - received max_messages number of messages or more
        - taken longer than the specified timeout condition
        """
        connection = self.get_connection()

        with connection.cursor() as cur:
            for channel in self.channels:
                cur.execute(self.get_listen_query(channel))
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                connected_callback()

            logger.debug('Starting listening for pg_notify notifications')
            for notify in connection.notifies(timeout=timeout, stop_after=max_messages):
                yield (notify.channel, notify.payload)

            cur.execute(self.get_unlisten_query())

    def publish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        connection = self.get_connection()
        channel = self.get_publish_channel(channel)

        with connection.cursor() as cur:
            cur.execute(self.NOTIFY_QUERY_TEMPLATE, (channel, message))

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    def close(self) -> None:
        if self.owns_sync_connection and self._sync_connection:
            logger.info('Closing synchronous psycopg connection')
            self._sync_connection.close()
            self._sync_connection = None
            self.owns_sync_connection = False


class ConnectionSaver:
    def __init__(self) -> None:
        self._connection: Optional[psycopg.Connection] = None
        self._async_connection: Optional[psycopg.AsyncConnection] = None
        self._lock = threading.Lock()


connection_save = ConnectionSaver()


def connection_saver(**config) -> psycopg.Connection:  # type: ignore[no-untyped-def]
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcherd does not manage connections, so this a simulation of that.

    Uses a thread lock to ensure thread safety.
    """
    with connection_save._lock:
        # Check if we need to create a new connection because it's either None or closed.
        if connection_save._connection is None or getattr(connection_save._connection, 'closed', False):
            connection_save._connection = create_connection(**config)
        return connection_save._connection


async def async_connection_saver(**config) -> psycopg.AsyncConnection:  # type: ignore[no-untyped-def]
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcherd does not manage connections, so this a simulation of that.

    Uses a thread lock to ensure thread safety.
    """
    with connection_save._lock:
        if connection_save._async_connection is None or getattr(connection_save._async_connection, 'closed', False):
            connection_save._async_connection = await acreate_connection(**config)
        return connection_save._async_connection
