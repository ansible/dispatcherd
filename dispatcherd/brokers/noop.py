import asyncio
import logging
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Union

from ..protocols import Broker as BrokerProtocol
from ..protocols import BrokerSelfCheckStatus

logger = logging.getLogger(__name__)


class Broker(BrokerProtocol):
    """A no-op broker that implements the Broker protocol but does nothing.

    This broker is useful for testing or when you want to disable message passing
    without changing the code that uses the broker interface.
    """

    def __init__(self) -> None:
        self.self_check_status = BrokerSelfCheckStatus.IDLE

    def __str__(self) -> str:
        return 'noop-broker'

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[Union[int, str], str], None]:
        """No-op implementation that yields once after the forever loop."""
        if connected_callback:
            await connected_callback()
        # Never yield, allowing the no-op broker to coexist with other brokers
        while True:
            await asyncio.sleep(0.1)  # Prevent busy-waiting
        yield ('', '')  # Yield once to satisfy the AsyncGenerator return type

    async def apublish_message(self, channel: Optional[str] = None, origin: Union[int, str, None] = None, message: str = '') -> None:
        """No-op implementation that does nothing."""
        logger.debug(f'No-op broker ignoring message of length {len(message)}')

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
        """No-op implementation that returns an empty string."""
        logger.debug(f'No-op broker ignoring message of length {len(message) if message else 0}')
        return ''

    def close(self) -> None:
        """No-op implementation that does nothing."""
        pass

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """No-op implementation that does nothing."""
        pass
