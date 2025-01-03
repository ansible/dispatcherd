import logging
from typing import Callable, Iterable, Optional

import psycopg

from dispatcher.brokers.base import BaseBroker
from dispatcher.utils import resolve_callable

logger = logging.getLogger(__name__)


"""This module exists under the theory that dispatcher messaging should be swappable

to different message busses eventually.
That means that the main code should never import psycopg.
Thus, all psycopg-lib-specific actions must happen here.
"""


class PGNotifyBase(BaseBroker):

    def __init__(
        self,
        config: Optional[dict] = None,
        channels: Iterable[str] = ('dispatcher_default',),
        default_publish_channel: Optional[str] = None,
    ) -> None:
        """
        channels - listening channels for the service and used for control-and-reply
        default_publish_channel - if not specified on task level or in the submission
          by default messages will be sent to this channel.
          this should be one of the listening channels for messages to be received.
        """
        if config:
            self._config: dict = config.copy()
            self._config['autocommit'] = True
        else:
            self._config = {}

        self.channels = channels
        self.default_publish_channel = default_publish_channel

    def get_publish_channel(self, channel: Optional[str] = None):
        "Handle default for the publishing channel for calls to publish_message, shared sync and async"
        if channel is not None:
            return channel
        if self.default_publish_channel is None:
            raise ValueError('Could not determine a channel to use publish to from settings or PGNotify config')
        return self.default_publish_channel

    def get_connection_method(self, factory_path: Optional[str] = None) -> Callable:
        "Handles settings, returns a method (async or sync) for getting a new connection"
        if factory_path:
            factory = resolve_callable(factory_path)
            if not factory:
                raise RuntimeError(f'Could not import connection factory {factory_path}')
            return factory
        elif self._config:
            return self.create_connection
        else:
            raise RuntimeError('Could not construct connection for lack of config or factory')

    def create_connection(self): ...


class AsyncBroker(PGNotifyBase):
    def __init__(
        self,
        config: Optional[dict] = None,
        async_connection_factory: Optional[str] = None,
        sync_connection_factory: Optional[str] = None,  # noqa
        connection: Optional[psycopg.AsyncConnection] = None,
        **kwargs,
    ) -> None:
        if not (config or async_connection_factory or connection):
            raise RuntimeError('Must specify either config or async_connection_factory')

        self._async_connection_factory = async_connection_factory
        self._connection = connection

        super().__init__(config=config, **kwargs)

    async def get_connection(self) -> psycopg.AsyncConnection:
        if not self._connection:
            factory = self.get_connection_method(factory_path=self._async_connection_factory)
            connection = await factory(**self._config)
            self._connection = connection
            return connection  # slightly weird due to MyPY
        return self._connection

    @staticmethod
    async def create_connection(**config) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(**config)

    async def aprocess_notify(self, connected_callback=None):
        connection = await self.get_connection()
        async with connection.cursor() as cur:
            for channel in self.channels:
                await cur.execute(f"LISTEN {channel};")
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                await connected_callback()

            while True:
                logger.debug('Starting listening for pg_notify notifications')
                async for notify in connection.notifies():
                    yield notify.channel, notify.payload

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        connection = await self.get_connection()
        channel = self.get_publish_channel(channel)

        async with connection.cursor() as cur:
            if not message:
                await cur.execute(f'NOTIFY {channel};')
            else:
                await cur.execute(f"NOTIFY {channel}, '{message}';")

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    async def aclose(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None


class SyncBroker(PGNotifyBase):
    def __init__(
        self,
        config: Optional[dict] = None,
        async_connection_factory: Optional[str] = None,  # noqa
        sync_connection_factory: Optional[str] = None,
        connection: Optional[psycopg.Connection] = None,
        **kwargs,
    ) -> None:
        if not (config or sync_connection_factory or connection):
            raise RuntimeError('Must specify either config or async_connection_factory')

        self._sync_connection_factory = sync_connection_factory
        self._connection = connection
        super().__init__(config=config, **kwargs)

    def get_connection(self) -> psycopg.Connection:
        if not self._connection:
            factory = self.get_connection_method(factory_path=self._sync_connection_factory)
            connection = factory(**self._config)
            self._connection = connection
            return connection
        return self._connection

    @staticmethod
    def create_connection(**config) -> psycopg.Connection:
        return psycopg.Connection.connect(**config)

    def publish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        connection = self.get_connection()
        channel = self.get_publish_channel(channel)

        with connection.cursor() as cur:
            if message:
                cur.execute('SELECT pg_notify(%s, %s);', (channel, message))
            else:
                cur.execute(f'NOTIFY {channel};')

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None


class ConnectionSaver:
    def __init__(self) -> None:
        self._connection: Optional[psycopg.Connection] = None
        self._async_connection: Optional[psycopg.AsyncConnection] = None


connection_save = ConnectionSaver()


def connection_saver(**config) -> psycopg.Connection:
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcher does not manage connections, so this a simulation of that.
    """
    if connection_save._connection is None:
        config['autocommit'] = True
        connection_save._connection = SyncBroker.create_connection(**config)
    return connection_save._connection


async def async_connection_saver(**config) -> psycopg.AsyncConnection:
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcher does not manage connections, so this a simulation of that.
    """
    if connection_save._async_connection is None:
        config['autocommit'] = True
        connection_save._async_connection = await AsyncBroker.create_connection(**config)
    return connection_save._async_connection
