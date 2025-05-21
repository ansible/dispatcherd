import asyncio
import logging
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Union

from ..protocols import Broker as BrokerProtocol
from ..protocols import BrokerSelfCheckStatus

logger = logging.getLogger(__name__)


class Broker(BrokerProtocol):
    """A broker that raises exceptions for publishing methods.

    This broker is useful for testing error handling or when you want to simulate
    publishing failures without changing the code that uses the broker interface.
    """

    def __init__(self, error_message: str = "Error-only broker: publishing is not allowed") -> None:
        self.error_message = error_message
        self.self_check_status = BrokerSelfCheckStatus.IDLE

    def __str__(self) -> str:
        return 'error-only-broker'

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[Union[int, str], str], None]:
        """No-op implementation that yields once after the forever loop."""
        if connected_callback:
            await connected_callback()
        # Never yield, allowing the error-only broker to coexist with other brokers
        while True:
            await asyncio.sleep(0.1)  # Prevent busy-waiting
        yield ('', '')  # Yield once to satisfy the AsyncGenerator return type

    async def apublish_message(self, channel: Optional[str] = None, origin: Union[int, str, None] = None, message: str = '') -> None:
        """Raises an exception when attempting to publish a message."""
        raise RuntimeError(self.error_message)

    async def aclose(self) -> None:
        """No-op implementation that does nothing."""
        pass

    def process_notify(
        self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1
    ) -> Iterator[tuple[Union[int, str], str]]:
        """No-op implementation that yields nothing."""
        if connected_callback:
            connected_callback()
        return iter([])

    def publish_message(self, channel: Optional[str] = None, message: Optional[str] = None) -> str:
        """Raises an exception when attempting to publish a message."""
        raise RuntimeError(self.error_message)

    def close(self) -> None:
        """No-op implementation that does nothing."""
        pass

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """No-op implementation that does nothing."""
        pass
