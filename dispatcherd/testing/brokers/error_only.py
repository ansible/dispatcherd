from typing import Optional

from .noop import Broker as NoOpBroker


class Broker(NoOpBroker):
    """A broker that inherits the NoOp behavior but raises when publishing."""

    def __init__(self, error_message: str = 'Error-only broker: publishing is not allowed') -> None:
        super().__init__()
        self.error_message = error_message

    def __str__(self) -> str:
        return 'error-only-broker'

    async def apublish_message(self, channel: Optional[str] = None, origin: int | str | None = None, message: str = '') -> None:
        raise RuntimeError(self.error_message)

    def publish_message(self, channel: Optional[str] = None, message: Optional[str] = None) -> str:
        raise RuntimeError(self.error_message)
