from typing import AsyncGenerator, Optional, Protocol


class BaseBroker(Protocol):
    async def aprocess_notify(self, connected_callback=None) -> AsyncGenerator[tuple[str, str], None]:
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None: ...

    async def aclose(self) -> None: ...

    def publish_message(self, channel=None, message=None): ...

    def close(self): ...
