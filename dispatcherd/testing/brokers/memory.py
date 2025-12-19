import asyncio
from collections import deque
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional

from ...protocols import Broker as BrokerProtocol
from ...protocols import BrokerSelfCheckStatus


class Broker(BrokerProtocol):
    """In-memory broker that records published messages for test assertions."""

    def __init__(self) -> None:
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.published_messages: list[dict[str, Any]] = []
        self._notify_queue: deque[tuple[int | str, str]] = deque()
        self._async_notify_queue: asyncio.Queue[tuple[int | str, str]] = asyncio.Queue()

    def __str__(self) -> str:
        return 'memory-broker'

    def queue_notification(self, origin: int | str, message: str) -> None:
        """Push a notification that will be yielded by notify iterators."""
        payload = (origin, message)
        self._notify_queue.append(payload)
        self._async_notify_queue.put_nowait(payload)

    def _record_publish(self, *, channel: Optional[str], origin: int | str | None, message: Optional[str], is_async: bool) -> None:
        self.published_messages.append(
            {
                'channel': channel,
                'origin': origin,
                'message': message,
                'is_async': is_async,
            }
        )

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[int | str, str], None]:
        """Yield queued notifications for asynchronous consumers."""
        if connected_callback:
            await connected_callback()
        while True:
            payload = await self._async_notify_queue.get()
            yield payload

    async def apublish_message(self, channel: Optional[str] = None, origin: int | str | None = None, message: str = '') -> None:
        """Record asynchronously published messages."""
        self._record_publish(channel=channel, origin=origin, message=message, is_async=True)

    async def aclose(self) -> None:
        """No-op implementation for compatibility."""
        return None

    def process_notify(
        self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int | None = 1
    ) -> Iterator[tuple[int | str, str]]:
        """Yield queued notifications to synchronous consumers."""
        if connected_callback:
            connected_callback()
        count = 0
        while self._notify_queue and (max_messages is None or count < max_messages):
            count += 1
            yield self._notify_queue.popleft()

    def publish_message(self, channel: Optional[str] = None, message: Optional[str] = None) -> str:
        """Record synchronously published messages."""
        self._record_publish(channel=channel, origin=None, message=message, is_async=False)
        return message or ''

    def close(self) -> None:
        """No-op implementation for compatibility."""
        return None

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """No-op implementation for compatibility."""
        return None
