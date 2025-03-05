import asyncio
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterable, Iterator, Optional, Protocol, Union


class Broker(Protocol):
    async def aprocess_notify(
        self, connected_callback: Optional[Optional[Callable[[], Coroutine[Any, Any, None]]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None: ...

    async def aclose(self) -> None: ...

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]: ...

    def publish_message(self, channel=None, message=None): ...

    def close(self): ...


class ProducerEvents(Protocol):
    ready_event: asyncio.Event


class Producer(Protocol):
    events: ProducerEvents

    async def start_producing(self, dispatcher: 'DispatcherMain') -> None: ...

    async def shutdown(self): ...

    def all_tasks(self) -> Iterable[asyncio.Task]: ...


class WorkerPool(Protocol):
    async def start_working(self, forking_lock: asyncio.Lock) -> None: ...

    async def dispatch_task(self, message: dict) -> None: ...


class DispatcherMain(Protocol):
    fd_lock: asyncio.Lock

    async def main(self) -> None: ...

    async def connected_callback(self, producer: Producer) -> None:
        """Called by producers when they are connected"""
        ...

    async def process_message(
        self, payload: Union[dict, str], producer: Optional[Producer] = None, channel: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        """This is called by producers to trigger a new task"""
        ...
