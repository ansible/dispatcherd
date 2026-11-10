"""Best-effort dispatcherd transport over local SQL Server Service Broker dialogs."""

import asyncio
import importlib
import json
import logging
import random
import sys
import threading
import time
import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from typing import Any, TypeVar

import aioodbc  # type: ignore[import-not-found,import-untyped]
import pyodbc  # type: ignore[import-not-found]

from ...chunking import split_message
from ...protocols import Broker as BrokerProtocol

logger = logging.getLogger(__name__)

T = TypeVar("T")
REPLY_PREFIX = "sqlserver-reply:"
REPLY_LIFETIME = 300


def connect(config: dict) -> pyodbc.Connection:
    options = dict(config)
    connection_string = options.pop("connection_string")
    options.setdefault("autocommit", True)
    options.setdefault("timeout", 10)
    connection = pyodbc.connect(connection_string, **options)
    connection.timeout = 35
    return connection


def _rows(connection: pyodbc.Connection, sql: str, *params: Any) -> list[dict[str, Any]]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *params)
        result: list[dict[str, Any]] = []
        while True:
            if cursor.description:
                names = [col[0] for col in cursor.description]
                result.extend(dict(zip(names, row)) for row in cursor.fetchall())
            if not cursor.nextset():
                break
        return result
    finally:
        cursor.close()


async def _complete_io(operation: Coroutine[Any, Any, T]) -> T:
    """Keep one aioodbc operation alive through cancellation until it is safe to close.

    aioodbc owns ODBC execution. Cancelling its executor future does not stop SQL;
    this boundary joins cursor/connection work before releasing its asyncio lock.
    No synchronous broker methods are run in an executor here.
    """
    task = asyncio.create_task(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:  # noqa: BLE001 - cancellation wins; retrieve exception below
                break
        if not task.cancelled():
            task.exception()
        raise


async def _arows(connection: aioodbc.Connection, sql: str, *params: Any) -> list[dict[str, Any]]:
    async def execute() -> list[dict[str, Any]]:
        # Do not use aioodbc's transaction-managing context manager on borrowed
        # connections: commits and rollbacks belong to their caller.
        cursor = await connection.cursor()
        try:
            await cursor.execute(sql, *params)
            result: list[dict[str, Any]] = []
            while True:
                if cursor.description:
                    names = [col[0] for col in cursor.description]
                    result.extend(dict(zip(names, row)) for row in await cursor.fetchall())
                if not await cursor.nextset():
                    break
            return result
        finally:
            await cursor.close()

    return await _complete_io(execute())


def reply_handle(address: str) -> str | None:
    if not address.startswith(REPLY_PREFIX):
        return None
    # A routing token is data, never executable SQL or an arbitrary service name.
    return str(uuid.UUID(address.removeprefix(REPLY_PREFIX)))


def _factory(factory: str | Callable) -> Callable:
    if isinstance(factory, str):
        module, name = factory.rsplit('.', 1)
        return getattr(importlib.import_module(module), name)
    return factory


def _subscriptions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row['instance_id'] = str(row['instance_id']).lower()
        row['channels'] = [item['channel'] for item in json.loads(row['channels'])]
    return rows


def _recipients(rows: list[dict[str, Any]], mode: str, worker_id: str | None) -> list[str]:
    ids = [row['instance_id'] for row in rows]
    if mode == 'to':
        return [identity for identity in ids if identity == str(uuid.UUID(str(worker_id)))]
    if mode == 'any':
        return random.sample(ids, min(1, len(ids)))
    return ids


def validate_channel(channel: str | None) -> str:
    if not isinstance(channel, str) or not channel or len(channel.encode("utf-16-le")) > 512 or channel.endswith(" "):
        raise ValueError("Channels must contain 1–256 UTF-16 code units and no trailing spaces")
    return channel


class Broker(BrokerProtocol):
    """One registration per listening lifetime, not per subprocess worker.

    ``sync_connection`` and ``sync_connection_factory`` apply only to publishing;
    they remain caller owned, and their transactions are never committed here.
    Listener/admin connections are separate, owned, autocommit connections.
    Async operations use aioodbc and separate receive/outbound connections.
    A reply endpoint has no service registration and lives for at most five minutes.
    """

    def __init__(
        self,
        config: dict,
        channels: tuple | list = (),
        default_publish_channel: str | None = None,
        node_id: str | None = None,
        wait_timeout_ms: int = 1000,
        registration_timeout: float = 10,
        max_connection_idle_seconds: float | None = 30,
        max_self_check_message_age_seconds: float = 2,
        max_message_bytes: int | None = 65536,
        sync_connection: pyodbc.Connection | None = None,
        sync_connection_factory: str | Callable[..., pyodbc.Connection] | None = None,
        async_connection: aioodbc.Connection | None = None,
        async_connection_factory: str | Callable[..., Coroutine[Any, Any, aioodbc.Connection]] | None = None,
    ) -> None:
        if not 1 <= wait_timeout_ms <= 30000:
            raise ValueError("wait_timeout_ms must be between 1 and 30000")
        if registration_timeout <= 0:
            raise ValueError("registration_timeout must be positive")
        if node_id is not None and len(node_id.encode("utf-16-le")) > 512:
            raise ValueError("node_id exceeds nvarchar(256)")
        if max_connection_idle_seconds is not None and max_connection_idle_seconds <= 0:
            raise ValueError('max_connection_idle_seconds must be positive or None')
        if max_self_check_message_age_seconds <= 0:
            raise ValueError('max_self_check_message_age_seconds must be positive')
        self.max_connection_idle_seconds = max_connection_idle_seconds
        self.max_self_check_message_age_seconds = max_self_check_message_age_seconds
        self._check_token: str | None = None
        self._check_deadline = 0.0
        self._last_activity = time.monotonic()
        self._check_success_count = 0
        self.config = dict(config)
        self.channels = tuple(dict.fromkeys(validate_channel(c) for c in channels))
        if any(c.startswith(REPLY_PREFIX) for c in self.channels):
            raise ValueError('sqlserver-reply: is reserved for direct reply addresses, not subscriptions')
        self.default_publish_channel = default_publish_channel
        if default_publish_channel is not None:
            validate_channel(default_publish_channel)
        self.node_id = node_id
        self.wait_timeout_ms = wait_timeout_ms
        self.registration_timeout = registration_timeout
        self.max_message_bytes = max_message_bytes
        self.instance_id: str | None = None
        self.receiver_handle: str | None = None
        self._listener: pyodbc.Connection | None = None
        self._publisher = sync_connection
        self._factory = sync_connection_factory
        self._owns_publisher = sync_connection is None and sync_connection_factory is None
        self._listen_lock = threading.RLock()
        self._publish_lock = threading.RLock()
        self._iteration_lock = threading.Lock()
        self._stopping = threading.Event()
        self._async_listener: aioodbc.Connection | None = None
        self._async_publisher = async_connection
        self._async_factory = async_connection_factory
        self._owns_async_publisher = async_connection is None and async_connection_factory is None
        self._async_listen_lock = asyncio.Lock()
        self._async_publish_lock = asyncio.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._listener_mode: str | None = None
        self._reply_publisher: str | None = None
        self._reply_deadline: float | None = None
        self.reply_address: str | None = None

    @staticmethod
    def setup_database(config: dict) -> None:
        """Provision fixed objects in an existing, Service Broker-enabled database."""
        connection = connect({**config, "autocommit": True})
        try:
            row = _rows(
                connection,
                "SELECT is_broker_enabled FROM sys.databases WHERE database_id=DB_ID()",
            )[0]
            if not row["is_broker_enabled"]:
                raise RuntimeError("Service Broker is disabled; enable it explicitly before setup_database")
            # Serialize concurrent installers without granting DDL to runtime users.
            lock = _rows(
                connection,
                "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource='dispatcherd_setup', @LockMode='Exclusive', @LockOwner='Session', "
                "@LockTimeout=10000; SELECT @r AS result",
            )[0]
            if lock["result"] < 0:
                raise RuntimeError("Could not acquire provisioning lock")
            for batch in files("dispatcherd.brokers.sqlserver").joinpath("schema.sql").read_text().split("\nGO\n"):
                if batch.strip():
                    _rows(connection, batch)
            queues = _rows(
                connection,
                "SELECT name,is_receive_enabled,is_enqueue_enabled FROM sys.service_queues WHERE name IN ('DispatcherdInbox','DispatcherdOutbox')",
            )
            if len(queues) != 2 or any(not r["is_receive_enabled"] or not r["is_enqueue_enabled"] for r in queues):
                raise RuntimeError("Dispatcherd Service Broker queues are disabled")
        finally:
            connection.close()

    def get_publish_channel(self, channel: str | None = None) -> str:
        channel = channel if channel is not None else self.default_publish_channel
        if channel is None and len(self.channels) == 1:
            channel = self.channels[0]
        channel = validate_channel(channel)
        reply_handle(channel)  # reject malformed reserved reply addresses early
        return channel

    def _get_publisher(self) -> pyodbc.Connection:
        if self._publisher is None:
            self._publisher = _factory(self._factory)(**self.config) if self._factory else connect(self.config)
        return self._publisher

    def _claim_listener(self, mode: str) -> None:
        if self._listener_mode not in (None, mode):
            raise RuntimeError('Cannot mix synchronous and asynchronous receiving on one broker')
        self._listener_mode = mode

    @staticmethod
    def _reply_timeout(timeout: float) -> None:
        if not 0 < timeout < REPLY_LIFETIME:
            raise ValueError('Control reply timeout must be positive and less than 300 seconds')

    def open_reply_endpoint(self, timeout: float) -> str:
        """Optional Control capability: allocate a direct return address, not a worker."""
        self._reply_timeout(timeout)
        with self._listen_lock:
            self._claim_listener('sync')
            if self._listener is not None:
                raise RuntimeError('A receive endpoint is already open')
            self._stopping.clear()
            self._reply_deadline = time.monotonic() + timeout
            self._listener = connect({**self.config, 'autocommit': True})
            try:
                row = _rows(self._listener, 'EXEC dbo.DispatcherdOpenReply')[0]
                self._reply_publisher = str(row['publisher_handle'])
                self.reply_address = REPLY_PREFIX + self._reply_publisher
                deadline = min(self._reply_deadline, time.monotonic() + self.registration_timeout)
                while time.monotonic() < deadline:
                    rows = _rows(self._listener, 'EXEC dbo.DispatcherdReplyReady ?', self._reply_publisher)
                    if rows:
                        self.receiver_handle = str(rows[0]['receiver_handle'])
                        return self.reply_address
                    time.sleep(0.02)
                raise TimeoutError('Reply endpoint did not become ready')
            except BaseException:
                self._close_listener(suppress_errors=True)
                raise

    def snapshot(self, channel: str | None = None) -> list[dict[str, Any]]:
        """Snapshot registrations; endpoint handles are derived from the SQL catalog."""
        if channel is not None:
            validate_channel(channel)
        with self._publish_lock:
            rows = _rows(self._get_publisher(), "EXEC dbo.DispatcherdSnapshot ?", channel)
        return _subscriptions(rows)

    def register(self) -> str:
        """Register a conversation and verify the initial self-check; idempotent locally."""
        with self._listen_lock:
            self._claim_listener('sync')
            if self._reply_publisher is not None:
                assert self.reply_address is not None
                return self.reply_address
            if self.instance_id is not None:
                return self.instance_id
            if not self.channels:
                raise ValueError("A listener needs at least one subscription")
            self._stopping.clear()
            self._listener = connect({**self.config, "autocommit": True})
            try:
                body = self._new_self_check(self.registration_timeout)
                row = _rows(self._listener, 'EXEC dbo.DispatcherdRegister ?,?,?', self.node_id, json.dumps(self.channels), body)[0]
                self.instance_id = str(row['instance_id']).lower()
                while self._check_token:
                    self._receive(self._receive_timeout())
                return self.instance_id
            except BaseException:
                self._close_listener(suppress_errors=True)
                raise

    def _payloads(self, channel: str, message: str) -> list[bytes]:
        return [
            json.dumps({'channel': channel, 'message': chunk}, ensure_ascii=False).encode('utf-8')
            for chunk in split_message(message, max_bytes=self.max_message_bytes)
        ]

    def _send(self, channel: str | None, message: str, mode: str, worker_id: str | None = None) -> list[str]:
        channel = self.get_publish_channel(channel)
        handle = reply_handle(channel)
        bodies = self._payloads(channel, message)
        with self._publish_lock:
            recipients = [handle] if handle else _recipients(self.snapshot(channel), mode, worker_id)
            if not recipients and not handle:
                logger.warning('SQL Server publication has no recipients: channel=%s mode=%s target=%s', channel, mode, worker_id)
            delivered = []
            for recipient in recipients:
                for chunk_index, body in enumerate(bodies, 1):
                    if handle:
                        result = _rows(self._get_publisher(), 'EXEC dbo.DispatcherdSendReply ?,?', handle, body)
                    else:
                        result = _rows(self._get_publisher(), 'EXEC dbo.DispatcherdSend ?,?,?', recipient, channel, body)
                    if not result[0]['sent']:
                        log = logger.debug if handle else logger.warning
                        log('SQL Server send rejected: channel=%s destination=%s chunk=%s/%s', channel, recipient, chunk_index, len(bodies))
                        break
                else:
                    delivered.append(recipient)
            return delivered

    def broadcast(self, channel: str, message: str) -> list[str]:
        """Return registration IDs successfully sent to (not delivery acknowledgments)."""
        return self._send(channel, message, "all")

    def send_any(self, channel: str, message: str) -> str | None:
        recipients = self._send(channel, message, "any")
        return recipients[0] if recipients else None

    def send_to(self, worker_id: str, channel: str, message: str) -> bool:
        """Return False when the instance is unavailable or not subscribed."""
        return bool(self._send(channel, message, "to", worker_id))

    def publish_message(self, channel: str | None = None, message: str = "") -> str:
        channel = self.get_publish_channel(channel)
        self.broadcast(channel, message)
        return channel

    def _receive(self, timeout_ms: int) -> tuple[str, str] | None:
        with self._listen_lock:
            if self._stopping.is_set() or (self.instance_id is None and self.receiver_handle is None):
                return None
            assert self._listener is not None
            timeout_ms = min(timeout_ms, self._receive_timeout())
            sql, identity = self._receive_query(timeout_ms)
            rows = _rows(self._listener, sql, identity, timeout_ms)
            if not rows:
                return None
            row = rows[0]
            if row["message_type_name"] != "DispatcherdPayload":
                # Never hand EndDialog/Error (XML) to dispatcherd's JSON decoder.
                self._close_listener(suppress_errors=True)
                raise ConnectionError(f"Service Broker conversation ended: {row['message_type_name']}")
            envelope = json.loads(bytes(row["message_body"]).decode("utf-8"))
            self._last_activity = time.monotonic()
            if envelope.get('kind') == 'self_check':
                self.verify_self_check(envelope)
                return None
            if envelope.get("kind") == "probe":
                self.publish_message(
                    envelope['reply_address'],
                    json.dumps(
                        {
                            'correlation_id': envelope['correlation_id'],
                            'instance_id': self.instance_id,
                        }
                    ),
                )
                return None
            return envelope["channel"], envelope["message"]

    def process_notify(self, connected_callback: Callable | None = None, timeout: float = 5.0, max_messages: int | None = 1) -> Iterator[tuple[str, str]]:
        if not self._iteration_lock.acquire(blocking=False):
            raise RuntimeError("Only one receive loop is allowed per broker")
        try:
            self.register()
            if connected_callback:
                connected_callback()
            deadline = min(time.monotonic() + timeout, self._reply_deadline or float("inf"))
            count = 0
            while not self._stopping.is_set() and (max_messages is None or count < max_messages):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if self._self_check_due():
                    with self._listen_lock:
                        if self._listener is None or self._stopping.is_set():
                            break
                        self._new_self_check(self.max_self_check_message_age_seconds)
                        rows = _rows(self._listener, 'SELECT CAST(? AS nvarchar(36)) AS token', self._check_token)
                        if rows:
                            self.verify_self_check(rows[0])
                result = self._receive(max(1, min(self.wait_timeout_ms, int(remaining * 1000))))
                if result is not None:
                    count += 1
                    yield result
        finally:
            try:
                self._close_listener(suppress_errors=sys.exc_info()[0] is not None)
            finally:
                self._iteration_lock.release()

    def retire(self, instance_ids: Iterable[str]) -> None:
        """Explicit policy decision; nonresponse alone does not prove death."""
        connection = connect({**self.config, "autocommit": True})
        try:
            for instance_id in instance_ids:
                _rows(
                    connection,
                    "EXEC dbo.DispatcherdRetire ?",
                    str(uuid.UUID(str(instance_id))),
                )
        finally:
            connection.close()

    def cleanup_expired_replies(self) -> list[str]:
        """Retire expired ephemeral dialogs; never inspect or probe worker registrations."""
        connection = connect({**self.config, 'autocommit': True})
        try:
            handles = [str(r['publisher_handle']) for r in _rows(connection, 'EXEC dbo.DispatcherdExpiredReplies')]
            for handle in handles:
                _rows(connection, 'EXEC dbo.DispatcherdCloseReply ?', handle)
            return handles
        finally:
            connection.close()

    def _receive_query(self, timeout_ms: int) -> tuple[str, str | None]:
        proc = 'DispatcherdReceiveReply' if self._reply_publisher else 'DispatcherdReceive'
        identity = self.receiver_handle if self._reply_publisher else self.instance_id
        # SET options are restored when sp_executesql returns, including on errors.
        # LOCK_TIMEOUT requires a literal; only the bounded integer is interpolated.
        sql = (
            f"EXEC sys.sp_executesql N'SET LOCK_TIMEOUT {int(timeout_ms)}; EXEC dbo.{proc} @identity,@timeout', "
            "N'@identity uniqueidentifier,@timeout int', ?,?"
        )
        return sql, identity

    def _reset_listener(self) -> None:
        self._check_token = None
        self.instance_id = None
        self.receiver_handle = None
        self._reply_publisher = None
        self.reply_address = None
        self._reply_deadline = None
        self._listener_mode = None

    def _close_listener(self, *, suppress_errors: bool = False) -> None:
        if self._listener_mode == 'sync':
            self._stopping.set()
        with self._listen_lock:
            if self._listener is None:
                return
            identity = self._reply_publisher or self.instance_id
            errors = []
            try:
                if self._reply_publisher:
                    connection = connect({**self.config, 'autocommit': True})
                    try:
                        _rows(connection, 'EXEC dbo.DispatcherdCloseReply ?', self._reply_publisher)
                    finally:
                        connection.close()
                elif self.instance_id:
                    self.retire([self.instance_id])
            except Exception as exc:
                logger.warning('SQL Server retirement failed for %s; explicit cleanup may be required', identity, exc_info=True)
                errors.append(exc)
            try:
                self._listener.close()
            except Exception as exc:
                logger.warning('SQL Server listener close failed for %s', identity, exc_info=True)
                errors.append(exc)
            finally:
                self._listener = None
                self._reset_listener()
            if errors and not suppress_errors:
                raise errors[0]

    def close(self) -> None:
        try:
            self._close_listener(suppress_errors=sys.exc_info()[0] is not None)
        finally:
            with self._publish_lock:
                if self._publisher is not None and self._owns_publisher:
                    self._publisher.close()
                    self._publisher = None

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """Reactive connection health, held only in memory."""
        if message.get('token') != self._check_token or self._check_token is None:
            return
        if time.monotonic() >= self._check_deadline:
            raise ConnectionError('SQL Server self-check timed out')
        self._check_token = None
        self._check_success_count += 1
        self._last_activity = time.monotonic()

    def _new_self_check(self, timeout: float) -> bytes:
        self._check_token = str(uuid.uuid4())
        self._check_deadline = time.monotonic() + timeout
        return json.dumps({'kind': 'self_check', 'token': self._check_token}).encode()

    def _receive_timeout(self) -> int:
        if self._check_token:
            remaining = self._check_deadline - time.monotonic()
            if remaining <= 0:
                raise ConnectionError('SQL Server self-check timed out')
            return max(1, min(self.wait_timeout_ms, int(remaining * 1000)))
        if self.instance_id and self.max_connection_idle_seconds is not None:
            remaining = self.max_connection_idle_seconds - (time.monotonic() - self._last_activity)
            return max(1, min(self.wait_timeout_ms, int(remaining * 1000)))
        return self.wait_timeout_ms

    def _self_check_due(self) -> bool:
        return bool(
            self.instance_id
            and self._check_token is None
            and self.max_connection_idle_seconds is not None
            and time.monotonic() - self._last_activity >= self.max_connection_idle_seconds
        )

    def get_self_check_stats(self) -> dict[str, Any]:
        return {'enabled': self.max_connection_idle_seconds is not None, 'success_count': self._check_success_count}

    async def _aconnect(self) -> aioodbc.Connection:
        if self._executor is None:
            # WAITFOR must not occupy asyncio's default executor (also used by DNS).
            # Two workers let outbound work proceed while one receive is blocked.
            self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='dispatcherd-sqlserver')
        options = dict(self.config)
        dsn = options.pop('connection_string')
        options['autocommit'] = True
        options.setdefault('timeout', 10)

        async def configure(connection: pyodbc.Connection) -> None:
            connection.timeout = 35

        return await aioodbc.connect(dsn=dsn, executor=self._executor, after_created=configure, **options)

    async def _aget_publisher(self) -> aioodbc.Connection:
        # Called with _async_publish_lock held; assignment is inside the shield
        # so a cancelled connect cannot leave an unowned, live connection.
        async def acquire() -> None:
            if self._async_factory:
                self._async_publisher = await _factory(self._async_factory)(**self.config)
            else:
                self._async_publisher = await self._aconnect()

        if self._async_publisher is None:
            await _complete_io(acquire())
        assert self._async_publisher is not None
        return self._async_publisher

    async def asnapshot(self, channel: str | None = None) -> list[dict[str, Any]]:
        if channel is not None:
            validate_channel(channel)
        async with self._async_publish_lock:
            return _subscriptions(await _arows(await self._aget_publisher(), 'EXEC dbo.DispatcherdSnapshot ?', channel))

    async def aregister(self) -> str:
        async def register() -> str:
            self._claim_listener('async')
            if self._reply_publisher:
                assert self.reply_address is not None
                return self.reply_address
            if self.instance_id:
                return self.instance_id
            if not self.channels:
                raise ValueError('A listener needs at least one subscription')
            self._stopping.clear()
            self._async_listener = await self._aconnect()
            body = self._new_self_check(self.registration_timeout)
            row = (await _arows(self._async_listener, 'EXEC dbo.DispatcherdRegister ?,?,?', self.node_id, json.dumps(self.channels), body))[0]
            self.instance_id = str(row['instance_id']).lower()
            # The caller holds the receive lock; the initial check is always the
            # first message because it is sent in the registration transaction.
            while self._check_token:
                timeout_ms = self._receive_timeout()
                sql, identity = self._receive_query(timeout_ms)
                rows = await _arows(self._async_listener, sql, identity, timeout_ms)
                if rows:
                    row = rows[0]
                    if row['message_type_name'] != 'DispatcherdPayload':
                        raise ConnectionError('Initial self-check conversation ended')
                    self.verify_self_check(json.loads(bytes(row['message_body']).decode('utf-8')))
            self._last_activity = time.monotonic()
            return self.instance_id

        async with self._async_listen_lock:
            try:
                return await _complete_io(register())
            except BaseException:
                await _complete_io(self._aclose_listener_unlocked(suppress_errors=True))
                raise

    async def aopen_reply_endpoint(self, timeout: float) -> str:
        self._reply_timeout(timeout)

        async def open_endpoint() -> str:
            self._claim_listener('async')
            if self._async_listener is not None:
                raise RuntimeError('A receive endpoint is already open')
            self._stopping.clear()
            self._reply_deadline = time.monotonic() + timeout
            self._async_listener = await self._aconnect()
            row = (await _arows(self._async_listener, 'EXEC dbo.DispatcherdOpenReply'))[0]
            self._reply_publisher = str(row['publisher_handle'])
            self.reply_address = REPLY_PREFIX + self._reply_publisher
            deadline = min(self._reply_deadline, time.monotonic() + self.registration_timeout)
            while time.monotonic() < deadline:
                rows = await _arows(self._async_listener, 'EXEC dbo.DispatcherdReplyReady ?', self._reply_publisher)
                if rows:
                    self.receiver_handle = str(rows[0]['receiver_handle'])
                    return self.reply_address
                await asyncio.sleep(0.02)
            raise TimeoutError('Reply endpoint did not become ready')

        async with self._async_listen_lock:
            try:
                return await _complete_io(open_endpoint())
            except BaseException:
                await _complete_io(self._aclose_listener_unlocked(suppress_errors=True))
                raise

    async def _asend(self, channel: str | None, message: str, mode: str, worker_id: str | None = None) -> list[str]:
        channel = self.get_publish_channel(channel)
        handle = reply_handle(channel)
        bodies = self._payloads(channel, message)
        async with self._async_publish_lock:
            connection = await self._aget_publisher()
            if handle:
                recipients = [handle]
            else:
                rows = _subscriptions(await _arows(connection, 'EXEC dbo.DispatcherdSnapshot ?', channel))
                recipients = _recipients(rows, mode, worker_id)
            if not recipients and not handle:
                logger.warning('SQL Server publication has no recipients: channel=%s mode=%s target=%s', channel, mode, worker_id)
            delivered = []
            for recipient in recipients:
                for chunk_index, body in enumerate(bodies, 1):
                    if handle:
                        result = await _arows(connection, 'EXEC dbo.DispatcherdSendReply ?,?', handle, body)
                    else:
                        result = await _arows(connection, 'EXEC dbo.DispatcherdSend ?,?,?', recipient, channel, body)
                    if not result[0]['sent']:
                        log = logger.debug if handle else logger.warning
                        log('SQL Server send rejected: channel=%s destination=%s chunk=%s/%s', channel, recipient, chunk_index, len(bodies))
                        break
                else:
                    delivered.append(recipient)
            return delivered

    async def abroadcast(self, channel: str, message: str) -> list[str]:
        return await self._asend(channel, message, 'all')

    async def asend_any(self, channel: str, message: str) -> str | None:
        recipients = await self._asend(channel, message, 'any')
        return recipients[0] if recipients else None

    async def asend_to(self, worker_id: str, channel: str, message: str) -> bool:
        return bool(await self._asend(channel, message, 'to', worker_id))

    async def apublish_message(self, channel: str | None = None, origin: str | int | None = None, message: str = '') -> None:
        await self._asend(channel, message, 'all')

    async def _areceive(self, timeout_ms: int) -> tuple[str, str] | None:
        async with self._async_listen_lock:
            if self._stopping.is_set() or (self.instance_id is None and self.receiver_handle is None):
                return None
            assert self._async_listener is not None
            timeout_ms = min(timeout_ms, self._receive_timeout())
            sql, identity = self._receive_query(timeout_ms)
            rows = await _arows(self._async_listener, sql, identity, timeout_ms)
            if not rows:
                return None
            row = rows[0]
            if row['message_type_name'] != 'DispatcherdPayload':
                await _complete_io(self._aclose_listener_unlocked(suppress_errors=True))
                raise ConnectionError(f"Service Broker conversation ended: {row['message_type_name']}")
            envelope = json.loads(bytes(row['message_body']).decode('utf-8'))
            self._last_activity = time.monotonic()
            if envelope.get('kind') == 'self_check':
                self.verify_self_check(envelope)
                return None
        if envelope.get('kind') == 'probe':
            await self.apublish_message(
                envelope['reply_address'],
                message=json.dumps(
                    {
                        'correlation_id': envelope['correlation_id'],
                        'instance_id': self.instance_id,
                    }
                ),
            )
            return None
        return envelope['channel'], envelope['message']

    async def aprocess_notify(self, connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = None) -> AsyncGenerator[tuple[str, str], None]:
        if not self._iteration_lock.acquire(blocking=False):
            raise RuntimeError('Only one receive loop is allowed per broker')
        try:
            await self.aregister()
            if connected_callback:
                await connected_callback()
            while not self._stopping.is_set():
                remaining = (self._reply_deadline or float('inf')) - time.monotonic()
                if remaining <= 0:
                    return
                timeout_ms = self.wait_timeout_ms if remaining == float('inf') else max(1, min(self.wait_timeout_ms, int(remaining * 1000)))
                if self._self_check_due():
                    async with self._async_listen_lock:
                        if self._async_listener is None or self._stopping.is_set():
                            break
                        self._new_self_check(self.max_self_check_message_age_seconds)
                        rows = await _arows(self._async_listener, 'SELECT CAST(? AS nvarchar(36)) AS token', self._check_token)
                        if rows:
                            self.verify_self_check(rows[0])
                result = await self._areceive(timeout_ms)
                if result is not None:
                    yield result
        finally:
            try:
                async with self._async_listen_lock:
                    await _complete_io(self._aclose_listener_unlocked(suppress_errors=sys.exc_info()[0] is not None))
            finally:
                self._iteration_lock.release()

    async def _admin(self, procedure: str, identities: Iterable[str]) -> None:
        # Administrative cleanup must commit independently of any borrowed
        # transactional publishing connection.
        async def execute() -> None:
            connection = await self._aconnect()
            try:
                for identity in identities:
                    await _arows(connection, f'EXEC dbo.{procedure} ?', str(uuid.UUID(identity)))
            finally:
                await connection.close()

        await _complete_io(execute())

    async def aretire(self, instance_ids: Iterable[str]) -> None:
        await self._admin('DispatcherdRetire', list(instance_ids))

    async def acleanup_expired_replies(self) -> list[str]:
        async def cleanup() -> list[str]:
            connection = await self._aconnect()
            try:
                handles = [str(r['publisher_handle']) for r in await _arows(connection, 'EXEC dbo.DispatcherdExpiredReplies')]
                for handle in handles:
                    await _arows(connection, 'EXEC dbo.DispatcherdCloseReply ?', handle)
                return handles
            finally:
                await connection.close()

        return await _complete_io(cleanup())

    async def probe(self, timeout: float = 2.0) -> dict[str, Any]:
        """Explicit correlated transport probe; retirement is a separate policy decision."""
        self._reply_timeout(timeout)
        snapshot = await self.asnapshot()
        correlation = str(uuid.uuid4())
        reply = Broker(self.config, wait_timeout_ms=self.wait_timeout_ms)
        responded: set[str] = set()
        try:
            address = await reply.aopen_reply_endpoint(timeout)
            async with self._async_publish_lock:
                connection = await self._aget_publisher()
                for row in snapshot:
                    if not row['channels']:
                        continue
                    body = json.dumps({'kind': 'probe', 'correlation_id': correlation, 'reply_address': address}).encode()
                    await _arows(connection, 'EXEC dbo.DispatcherdSend ?,?,?', row['instance_id'], row['channels'][0], body)
            deadline = time.monotonic() + timeout
            ids = {r['instance_id'] for r in snapshot}
            while time.monotonic() < deadline and responded != ids:
                result = await reply._areceive(max(1, min(self.wait_timeout_ms, int((deadline - time.monotonic()) * 1000))))
                if result:
                    data = json.loads(result[1])
                    if data.get('correlation_id') == correlation and data.get('instance_id') in ids:
                        responded.add(data['instance_id'])
            return {'correlation_id': correlation, 'snapshot': snapshot, 'responded': sorted(responded), 'suspected_stale': sorted(ids - responded)}
        finally:
            await reply.aclose()

    async def _aclose_listener_unlocked(self, *, suppress_errors: bool = False) -> None:
        if self._listener_mode == 'async':
            self._stopping.set()
        if self._async_listener is None:
            return
        identity = self._reply_publisher or self.instance_id
        errors = []
        try:
            if self._reply_publisher:
                await self._admin('DispatcherdCloseReply', [self._reply_publisher])
            elif self.instance_id:
                await self.aretire([self.instance_id])
        except Exception as exc:
            logger.warning('SQL Server retirement failed for %s; explicit cleanup may be required', identity, exc_info=True)
            errors.append(exc)
        try:
            await self._async_listener.close()
        except Exception as exc:
            logger.warning('SQL Server listener close failed for %s', identity, exc_info=True)
            errors.append(exc)
        finally:
            self._async_listener = None
            self._reset_listener()
        if errors and not suppress_errors:
            raise errors[0]

    async def aclose(self) -> None:
        suppress_errors = sys.exc_info()[0] is not None

        async def close() -> None:
            try:
                async with self._async_listen_lock:
                    await self._aclose_listener_unlocked(suppress_errors=suppress_errors)
            finally:
                async with self._async_publish_lock:
                    if self._async_publisher is not None and self._owns_async_publisher:
                        await self._async_publisher.close()
                        self._async_publisher = None
                if self._executor is not None:
                    # All operations above have settled; there is nothing to join
                    # on the event-loop thread. A later reuse creates a new executor.
                    self._executor.shutdown(wait=False)
                    self._executor = None

        if self._listener_mode == 'async':
            self._stopping.set()
        await _complete_io(close())
