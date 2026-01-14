import json
import logging
import threading
import time
import uuid
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator

import psycopg

from ..chunking import split_message
from ..protocols import Broker as BrokerProtocol
from ..protocols import BrokerSelfCheckStatus
from ..utils import resolve_callable

logger = logging.getLogger(__name__)

PG_NOTIFY_MAX_PAYLOAD_BYTES = 8000


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


class DispatcherdInvalidChannel(psycopg.errors.SyntaxError):
    pass


def validate_channel_name(channel_name: str) -> None:
    """Raise an exception if the channel name can not be reliably used.

    This might happen due to a number of reasons.
    The notify logic uses the channel name as a parameter, which is good for
      security reasons, but imposes a character length constraint.
    """
    if len(channel_name.encode('utf-8')) > 63:
        raise DispatcherdInvalidChannel(f'Channel name is too long chars={len(channel_name.encode("utf-8"))}')

    if not channel_name:
        raise DispatcherdInvalidChannel(f'Received blank channel name {channel_name}. PG notify channel name can not be blank.')


class Broker(BrokerProtocol):
    NOTIFY_QUERY_TEMPLATE = 'SELECT pg_notify(%s, %s);'

    def __init__(
        self,
        config: dict | None = None,
        async_connection_factory: str | None = None,
        sync_connection_factory: str | None = None,
        sync_connection: psycopg.Connection | None = None,
        async_connection: psycopg.AsyncConnection | None = None,
        channels: tuple | list = (),
        default_publish_channel: str | None = None,
        max_connection_idle_seconds: int | None = 30,
        max_self_check_message_age_seconds: int | None = 2,
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

        if max_connection_idle_seconds is not None:
            if max_self_check_message_age_seconds is None:
                raise RuntimeError('max_self_check_message_age_seconds must be specified if health checks are enabled')
            if max_self_check_message_age_seconds > max_connection_idle_seconds:
                raise RuntimeError('max_self_check_message_age_seconds must be smaller than max_connection_idle_seconds')
        else:
            max_self_check_message_age_seconds = None

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
        server_channels = list(channels)
        self.self_check_channel: str | None
        if self.max_connection_idle_seconds is not None:
            # Generate a special channel for broker self checks
            self.self_check_channel = self.generate_self_check_channel_name()
            if self.self_check_channel not in server_channels:
                server_channels.append(self.self_check_channel)
        else:
            self.self_check_channel = None
        self.channels = server_channels

        # Raise an early error if any of the channel names are invalid
        for channel in self.channels:
            validate_channel_name(channel)

        self.default_publish_channel = default_publish_channel
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic() if self.max_connection_idle_seconds is not None else None

        # If we are in the notification loop (receiving messages),
        # then we have to break out before sending messages
        # These variables track things so that we can exit, send, and re-enter
        self.notify_loop_active: bool = False
        self.notify_queue: list = []
        self.max_message_bytes = PG_NOTIFY_MAX_PAYLOAD_BYTES
        # Track self-check outcomes for status reporting
        self.self_check_success_count = 0
        self.self_check_success_total_duration = 0.0
        self.self_check_success_max_duration = 0.0

    @classmethod
    def generate_self_check_channel_name(cls) -> str:
        return f"self_check_{str(uuid.uuid4()).replace('-', '_')}"

    def get_publish_channel(self, channel: str | None = None) -> str:
        "Handle default for the publishing channel for calls to publish_message, shared sync and async"
        if channel is not None:
            return_channel = channel
        elif self.default_publish_channel is not None:
            return_channel = self.default_publish_channel
        elif len(self.user_channels) == 1:
            # de-facto default channel, because there is only 1
            return_channel = self.channels[0]
        else:
            raise ValueError('Could not determine a channel to use publish to from settings or PGNotify config')

        validate_channel_name(return_channel)
        return return_channel

    def __str__(self) -> str:
        return 'pg_notify-broker'

    # --- asyncio connection methods ---

    async def aget_connection(self) -> psycopg.AsyncConnection:
        # Check if the cached async connection is either None or closed.
        if not self._async_connection or getattr(self._async_connection, "closed", 0) != 0:
            start = time.perf_counter()
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
            logger.info('pg_notify async connection established in %.3f seconds', time.perf_counter() - start)
        assert self._async_connection is not None
        return self._async_connection

    def get_listen_query(self, channel: str) -> psycopg.sql.Composed:
        """Returns SQL command for listening on pg_notify channel

        This uses the psycopg utilities which ensure correct escaping so SQL injection is not possible.
        Return value is a valid argument for cursor.execute()
        """
        # Postgres does not allow parameters for identifiers, so this limits what channel we can accept
        validate_channel_name(channel)
        return psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel))

    def get_unlisten_query(self) -> psycopg.sql.SQL:
        """Stops listening on all channels for current session, see pg_notify docs"""
        return psycopg.sql.SQL("UNLISTEN *;")

    async def initiate_self_check(self) -> None:
        if self.max_connection_idle_seconds is None:
            logger.debug('Self check initiation skipped because self checks are disabled')
            return
        if self.self_check_status == BrokerSelfCheckStatus.IN_PROGRESS:
            # another self-check message is in flight
            assert self.last_self_check_message_time is not None
            delta = time.monotonic() - self.last_self_check_message_time
            raise RuntimeError(f'self check message for broker {self.broker_id} did not arrive in {delta} seconds')

        assert self.self_check_channel is not None
        await self.apublish_message(channel=self.self_check_channel, message=json.dumps({'self_check': True, 'task': f'lambda: "{self.broker_id}"'}))
        self.self_check_status = BrokerSelfCheckStatus.IN_PROGRESS
        self.last_self_check_message_time = time.monotonic()

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """Verify a received self check message: check if it was sent from the same node and
        is not outdated
        """
        if self.max_connection_idle_seconds is None:
            logger.debug('Self check verification skipped because self checks are disabled')
            return
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
            self._record_self_check_success(delta_seconds)
            logger.debug(f'self check succeeded, message received after {round(delta_seconds, 2)} seconds, broker-id {self.broker_id}')
        else:
            raise RuntimeError(f'self check failed, message received after {round(delta_seconds, 2)} seconds, broker-id {self.broker_id}')

    async def aprocess_notify(
        self, connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = None
    ) -> AsyncGenerator[tuple[str, str], None]:  # public
        connection = await self.aget_connection()
        async with connection.cursor() as cur:
            try:
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
                        if self.max_connection_idle_seconds is not None:
                            logger.debug(
                                f'No message received for {self.max_connection_idle_seconds} seconds, starting self check to channel={self.self_check_channel}'
                            )
                            await self.initiate_self_check()

                    self.notify_loop_active = False
                    for reply_to, reply_message in self.notify_queue:
                        await self.apublish_message_from_cursor(cur, channel=reply_to, message=reply_message)
                    self.notify_queue = []
            finally:
                try:
                    await cur.execute(self.get_unlisten_query())
                except Exception as exc:  # soft failure is fine, we're already exiting the loop
                    logger.warning('Failed to UNLISTEN during async cleanup: %s', exc)

    async def apublish_message_from_cursor(self, cursor: psycopg.AsyncCursor, channel: str | None = None, message: str = '') -> None:
        """The inner logic of async message publishing where we already have a cursor"""
        await cursor.execute(self.NOTIFY_QUERY_TEMPLATE, (channel, message))

    async def apublish_message(self, channel: str | None = None, origin: str | int | None = '', message: str = '') -> None:  # public
        """asyncio way to publish a message, used to send control in control-and-reply"""
        channel = self.get_publish_channel(channel)
        message_chunks = split_message(message, max_bytes=self.max_message_bytes)

        if self.notify_loop_active:
            for chunk in message_chunks:
                self.notify_queue.append((channel, chunk))
            return

        connection = await self.aget_connection()

        async with connection.cursor() as cur:
            for chunk in message_chunks:
                await self.apublish_message_from_cursor(cur, channel=channel, message=chunk)

        logger.debug(f'Sent pg_notify message of {len(message)} chars as {len(message_chunks)} chunk(s) to {channel}')

    async def aclose(self) -> None:
        if self.owns_async_connection and self._async_connection:
            logger.info('Closing asynchronous psycopg connection')
            await self._async_connection.close()
            self._async_connection = None
            self.owns_async_connection = False

        # Reset any server-related vars from __init__
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic() if self.max_connection_idle_seconds is not None else None
        self.notify_loop_active = False
        self.notify_queue = []

    # --- synchronous connection methods ---

    def get_connection(self) -> psycopg.Connection:
        # Check if the cached connection is either None or closed.
        if not self._sync_connection or getattr(self._sync_connection, "closed", 0) != 0:
            start = time.perf_counter()
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
            logger.info('pg_notify sync connection established in %.3f seconds', time.perf_counter() - start)
        assert self._sync_connection is not None
        return self._sync_connection

    def process_notify(self, connected_callback: Callable | None = None, timeout: float = 5.0, max_messages: int | None = 1) -> Iterator[tuple[str, str]]:
        """Blocking method that listens for messages on subscribed pg_notify channels until timeout

        This has two different exit conditions:
        - received max_messages number of messages or more
        - taken longer than the specified timeout condition
        """
        connection = self.get_connection()

        with connection.cursor() as cur:
            try:
                for channel in self.channels:
                    cur.execute(self.get_listen_query(channel))
                    logger.info(f"Set up pg_notify listening on channel '{channel}'")

                if connected_callback:
                    connected_callback()

                logger.debug('Starting listening for pg_notify notifications')
                for notify in connection.notifies(timeout=timeout, stop_after=max_messages):
                    yield (notify.channel, notify.payload)
            finally:
                # unlisten done in finally so that when the caller returns, connection does not risk getting unrelated messages
                try:
                    cur.execute(self.get_unlisten_query())
                except Exception as exc:
                    logger.warning('Failed to UNLISTEN during sync cleanup: %s', exc)

    def publish_message(self, channel: str | None = None, message: str = '') -> str:
        """Synchronous method to submit a message to a pg_notify channel, returns the queue it was sent to"""
        connection = self.get_connection()
        channel = self.get_publish_channel(channel)

        message_chunks = split_message(message, max_bytes=self.max_message_bytes)

        with connection.cursor() as cur:
            for chunk in message_chunks:
                cur.execute(self.NOTIFY_QUERY_TEMPLATE, (channel, chunk))

        logger.debug(f'Sent pg_notify message of {len(message)} chars as {len(message_chunks)} chunk(s) to {channel}')
        return channel

    def close(self) -> None:
        if self.owns_sync_connection and self._sync_connection:
            logger.info('Closing synchronous psycopg connection')
            self._sync_connection.close()
            self._sync_connection = None
            self.owns_sync_connection = False

    def _record_self_check_success(self, delta_seconds: float) -> None:
        self.self_check_success_count += 1
        self.self_check_success_total_duration += delta_seconds
        if delta_seconds > self.self_check_success_max_duration:
            self.self_check_success_max_duration = delta_seconds

    def get_self_check_stats(self) -> dict[str, Any]:
        """Return aggregated metrics for broker self-checks."""
        enabled = self.max_connection_idle_seconds is not None and self.max_self_check_message_age_seconds is not None
        success_count = self.self_check_success_count if enabled else 0
        average_time = None
        max_time = None
        if enabled and success_count:
            average_time = self.self_check_success_total_duration / success_count
            max_time = self.self_check_success_max_duration
        return {
            'enabled': enabled,
            'success_count': success_count,
            'average_time_seconds': average_time,
            'max_time_seconds': max_time,
        }


class ConnectionSaver:
    def __init__(self) -> None:
        self._connection: psycopg.Connection | None = None
        self._async_connection: psycopg.AsyncConnection | None = None
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
        # NOTE:  or getattr(connection_save._connection, 'closed', False) would fix some issues
        # but that is not done here for testing demonstration
        if connection_save._connection is None:
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
