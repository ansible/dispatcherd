import asyncio
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterable, Iterator, Optional, Protocol, Union


class Broker(Protocol):
    async def aprocess_notify(
        self, connected_callback: Optional[Optional[Callable[[], Coroutine[Any, Any, None]]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:
        """The generator of messages from the broker for the dispatcher service

        The producer iterates this to produce tasks.
        This uses the async connection of the broker.
        """
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        """Asynchronously send a message to the broker, used by dispatcher service for reply messages"""
        ...

    async def aclose(self) -> None:
        """Close the asynchronous connection, used by service, and optionally by publishers"""
        ...

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> Iterator[tuple[str, str]]:
        """Synchronous method to generate messages from broker, used for synchronous control-and-reply"""
        ...

    def publish_message(self, channel=None, message=None):
        """Synchronously publish message to broker, would be used by normal Django code to publish a task"""
        ...

    def close(self):
        """Close the sychronous connection"""
        ...


class ProducerEvents(Protocol):
    ready_event: asyncio.Event


class Producer(Protocol):
    events: ProducerEvents

    async def start_producing(self, dispatcher: 'DispatcherMain') -> None:
        """Starts tasks which will eventually call DispatcherMain.process_message - how tasks originate in the service"""
        ...

    async def shutdown(self):
        """Stop producing tasks and clean house, a producer may be shut down independently from the main program"""
        ...

    def all_tasks(self) -> Iterable[asyncio.Task]:
        """Returns all asyncio tasks, which is relevant for task management, shutdown, triggered from main loop"""
        ...


class WorkerPool(Protocol):
    async def start_working(self, forking_lock: asyncio.Lock) -> None:
        """Start persistent asyncio tasks, including asychronously starting worker subprocesses"""
        ...

    async def dispatch_task(self, message: dict) -> None:
        """Called by DispatcherMain after in the normal task lifecycle, pool will try to hand the task to a worker"""
        ...


class DispatcherMain(Protocol):
    fd_lock: asyncio.Lock

    async def main(self) -> None:
        """This is the method that runs the service, bring your own event loop"""
        ...

    async def connected_callback(self, producer: Producer) -> None:
        """Called by producers when they are connected"""
        ...

    async def process_message(
        self, payload: Union[dict, str], producer: Optional[Producer] = None, channel: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        """This is called by producers when a new request to run a task comes in"""
        ...
