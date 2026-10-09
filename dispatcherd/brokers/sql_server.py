"""SQL Server table-backed broker for dispatcherd."""

import asyncio
import logging
import threading
import time
from collections.abc import AsyncGenerator, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Coroutine

from ..protocols import Broker as BrokerProtocol
from ..utils import resolve_callable

logger = logging.getLogger(__name__)

MESSAGE_TABLE = "dbo.dispatcherd_messages"
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_QUERY_TIMEOUT_SECONDS = 30
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15


def _positive_timeout(config: dict[str, Any], key: str, default: int) -> int:
    timeout = int(config.get(key, default))
    if timeout <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return timeout


def _escape_odbc_value(value: Any) -> str:
    text = str(value)
    if not text:
        return "{}"
    if any(character in text for character in ";{}") or text != text.strip():
        return "{" + text.replace("}", "}}") + "}"
    return text


def _build_connection_string(config: dict[str, Any]) -> str:
    """Build an ODBC connection string without allowing values to add options."""
    server = str(config.get("server", "localhost"))
    port = config.get("port")
    if port:
        server = f"{server},{port}"

    driver = str(config.get("driver", DEFAULT_DRIVER)).replace("}", "}}")
    database = _escape_odbc_value(config.get("database", "dispatcherd"))
    user = _escape_odbc_value(config.get("user", "SA"))
    password = _escape_odbc_value(config.get("password", ""))
    trust_server_certificate = _escape_odbc_value(config.get("trust_server_certificate", "yes"))

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={_escape_odbc_value(server)};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate={trust_server_certificate};"
    )


def _create_connection(**config: Any) -> Any:
    """Create a synchronous pyodbc connection, importing the optional driver lazily."""
    try:
        import pyodbc  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("Install dispatcherd[sql_server] to use the SQL Server broker") from exc

    return pyodbc.connect(
        _build_connection_string(config),
        autocommit=True,
        timeout=_positive_timeout(config, "connect_timeout_seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS),
    )


def _is_closed(connection: Any) -> bool:
    return bool(getattr(connection, "closed", False))


class Broker(BrokerProtocol):
    """Persist channel messages in a SQL Server table and poll for consumers."""

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        async_connection_factory: str | Callable[..., Any] | None = None,
        sync_connection_factory: str | Callable[..., Any] | None = None,
        sync_connection: Any | None = None,
        async_connection: Any | None = None,
        channels: tuple[str, ...] | list[str] = (),
        default_publish_channel: str | None = None,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero")

        self._config = (config or {}).copy()
        self.connect_timeout_seconds = _positive_timeout(self._config, "connect_timeout_seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS)
        self.query_timeout_seconds = _positive_timeout(self._config, "query_timeout_seconds", DEFAULT_QUERY_TIMEOUT_SECONDS)
        self._sync_connection_factory = sync_connection_factory
        self._async_connection_factory = async_connection_factory
        self._sync_connection = sync_connection
        self._async_connection = async_connection
        self.owns_sync_connection = False
        self.owns_async_connection = False
        self._sync_lock = threading.Lock()
        self._async_executor: ThreadPoolExecutor | None = None
        self._async_executor_lock = threading.Lock()
        self._async_closed = False
        self._async_close_task: asyncio.Task | None = None

        self.channels = list(channels)
        self.user_channels = list(channels)
        self.default_publish_channel = default_publish_channel
        self.poll_interval_seconds = poll_interval_seconds

    def __str__(self) -> str:
        return "sql-server-table-broker"

    def get_publish_channel(self, channel: str | None = None) -> str:
        if channel is not None:
            return channel
        if self.default_publish_channel is not None:
            return self.default_publish_channel
        if len(self.user_channels) == 1:
            return self.user_channels[0]
        raise ValueError("Could not determine a channel to publish to from SQL Server broker config")

    def get_connection(self) -> Any:
        with self._sync_lock:
            if self._sync_connection is None or _is_closed(self._sync_connection):
                factory = self._resolve_factory(self._sync_connection_factory)
                if self._sync_connection_factory and factory is None:
                    raise RuntimeError(f"Could not import sync connection factory {self._sync_connection_factory}")
                self._sync_connection = factory(**self._config) if factory else _create_connection(**self._config)
                self.owns_sync_connection = True
                logger.info("SQL Server sync connection established")
            self._sync_connection.timeout = self.query_timeout_seconds
            return self._sync_connection

    @staticmethod
    def _resolve_factory(factory: str | Callable[..., Any] | None) -> Callable[..., Any] | None:
        if isinstance(factory, str):
            return resolve_callable(factory)
        return factory

    def _submit_async(self, function: Callable[..., Any], *args: Any) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        with self._async_executor_lock:
            if self._async_closed:
                raise RuntimeError("SQL Server async broker is closed")
            if self._async_executor is None:
                self._async_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dispatcherd-sql-server")
            # Submit while holding the same lock used by aclose so cleanup is queued after
            # every accepted operation and no operation can be queued after cleanup.
            return loop.run_in_executor(self._async_executor, partial(function, *args))

    async def _run_async(self, function: Callable[..., Any], *args: Any) -> Any:
        return await self._submit_async(function, *args)

    def _get_async_connection_blocking(self) -> Any:
        if self._async_connection is None or _is_closed(self._async_connection):
            factory = self._resolve_factory(self._async_connection_factory)
            if self._async_connection_factory and factory is None:
                raise RuntimeError(f"Could not import async connection factory {self._async_connection_factory}")
            self._async_connection = factory(**self._config) if factory else _create_connection(**self._config)
            self.owns_async_connection = True
            logger.info("SQL Server async connection established")
        self._async_connection.timeout = self.query_timeout_seconds
        return self._async_connection

    async def _get_async_connection(self) -> Any:
        return await self._run_async(self._get_async_connection_blocking)

    @staticmethod
    def _insert_message(connection: Any, channel: str, message: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MESSAGE_TABLE} (channel, payload) VALUES (?, ?)", (channel, message))

    def publish_message(self, channel: str | None = None, message: str = "") -> str:
        channel = self.get_publish_channel(channel)
        connection = self.get_connection()
        with self._sync_lock:
            self._insert_message(connection, channel, message)
        logger.debug("Inserted SQL Server broker message of %d chars to %s", len(message), channel)
        return channel

    async def apublish_message(self, channel: str | None = None, origin: int | str | None = None, message: str = "") -> None:
        channel = self.get_publish_channel(channel)
        connection = await self._get_async_connection()
        await self._run_async(self._insert_message, connection, channel, message)
        logger.debug("Inserted async SQL Server broker message of %d chars to %s", len(message), channel)

    @staticmethod
    def _dequeue_query(channel_count: int) -> str:
        channel_parameters = ", ".join("?" for _ in range(channel_count))
        return (
            ";WITH next_messages AS ("
            f" SELECT TOP (?) id, channel, payload FROM {MESSAGE_TABLE} WITH (UPDLOCK, READPAST, ROWLOCK)"
            f" WHERE channel IN ({channel_parameters}) ORDER BY id"
            ") "
            "DELETE FROM next_messages "
            "OUTPUT deleted.id, deleted.channel, deleted.payload;"
        )

    @classmethod
    def _dequeue_from_connection(cls, connection: Any, max_messages: int, channels: list[str]) -> list[tuple[int, str, str]]:
        if not channels or max_messages <= 0:
            return []

        query = cls._dequeue_query(len(channels))
        with connection.cursor() as cursor:
            cursor.execute(query, (max_messages, *channels))
            rows = cursor.fetchall()
        return sorted(((int(row[0]), str(row[1]), str(row[2])) for row in rows), key=lambda row: row[0])

    def _dequeue(self, max_messages: int, channels: list[str]) -> list[tuple[int, str, str]]:
        if not channels or max_messages <= 0:
            return []
        connection = self.get_connection()
        with self._sync_lock:
            return self._dequeue_from_connection(connection, max_messages, channels)

    def process_notify(
        self,
        connected_callback: Callable | None = None,
        timeout: float = 5.0,
        max_messages: int | None = 1,
    ) -> Iterator[tuple[str, str]]:
        if not self.channels or max_messages == 0:
            return

        self.get_connection()
        if connected_callback:
            connected_callback()

        deadline = time.monotonic() + max(0, timeout)
        received = 0
        while time.monotonic() < deadline:
            remaining = None if max_messages is None else max_messages - received
            if remaining is not None and remaining <= 0:
                return

            batch_size = 100 if remaining is None else remaining
            messages = self._dequeue(batch_size, self.channels)
            if messages:
                for _, channel, payload in messages:
                    yield channel, payload
                    received += 1
                continue

            time.sleep(min(self.poll_interval_seconds, max(0, deadline - time.monotonic())))

    async def aprocess_notify(
        self,
        connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = None,
    ) -> AsyncGenerator[tuple[str, str], None]:
        if not self.channels:
            return

        connection = await self._get_async_connection()
        if connected_callback:
            result = connected_callback()
            if asyncio.iscoroutine(result):
                await result

        try:
            while True:
                messages = await self._run_async(self._dequeue_from_connection, connection, 100, self.channels)
                if messages:
                    for _, channel, payload in messages:
                        yield channel, payload
                    continue
                await asyncio.sleep(self.poll_interval_seconds)
        finally:
            logger.debug("SQL Server async listener stopped")

    def close(self) -> None:
        if self.owns_sync_connection and self._sync_connection is not None:
            self._sync_connection.close()
            self._sync_connection = None
            self.owns_sync_connection = False

    async def aclose(self) -> None:
        with self._async_executor_lock:
            if self._async_close_task is None:
                self._async_closed = True
                executor, self._async_executor = self._async_executor, None
                self._async_close_task = asyncio.create_task(self._aclose_resources(executor))
            close_task = self._async_close_task
        await self._await_uncancellable(close_task)

    async def _aclose_resources(self, executor: ThreadPoolExecutor | None) -> None:
        if executor is None:
            await asyncio.to_thread(self._close_async_connection_blocking)
            return

        loop = asyncio.get_running_loop()
        # Queue cleanup on the connection's executor after in-flight connect/query work.
        # Connection creation may finish after its awaiting coroutine is cancelled.
        cleanup = loop.run_in_executor(executor, self._close_async_connection_blocking)
        try:
            await self._await_uncancellable(cleanup)
        finally:
            await self._await_uncancellable(asyncio.create_task(asyncio.to_thread(executor.shutdown, wait=True)))

    def _close_async_connection_blocking(self) -> None:
        connection = self._async_connection
        if connection is not None and self.owns_async_connection:
            try:
                connection.close()
            finally:
                self._async_connection = None
                self.owns_async_connection = False

    @staticmethod
    async def _await_uncancellable(awaitable: Any) -> Any:
        """Wait for resource cleanup to finish before propagating caller cancellation."""
        future = asyncio.ensure_future(awaitable)
        cancelled = False
        while True:
            try:
                await asyncio.shield(future)
                break
            except asyncio.CancelledError:
                cancelled = True
                if future.done():
                    break
        if cancelled:
            raise asyncio.CancelledError
        return future.result()
