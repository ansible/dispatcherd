from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional, Protocol


class BaseBroker(Protocol):
    async def aprocess_notify(
        self, connected_callback: Optional[Optional[Callable[[], Coroutine[Any, Any, None]]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None: ...

    async def aclose(self) -> None: ...

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]: ...

    def publish_message(self, channel=None, message=None): ...

    def close(self): ...
