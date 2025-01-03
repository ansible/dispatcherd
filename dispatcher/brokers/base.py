from abc import abstractmethod
from typing import Optional


class BaseBroker:
    @abstractmethod
    async def connect(self): ...

    @abstractmethod
    async def aprocess_notify(self, connected_callback=None): ...

    @abstractmethod
    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None: ...

    @abstractmethod
    async def aclose(self) -> None: ...

    @abstractmethod
    def get_connection(self): ...

    @abstractmethod
    def publish_message(self, channel=None, message=None): ...

    @abstractmethod
    def close(self): ...
