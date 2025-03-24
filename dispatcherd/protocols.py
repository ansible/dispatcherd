import asyncio
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterable, Iterator, Optional, Protocol, Union


class BrokerSelfCheckStatus(Enum):
    """This enum represents the result of a broker self-check"""

    IDLE = (1,)  # no self check is currently in progress
    IN_PROGRESS = (2,)  # a self check is currently in progress


class Broker(Protocol):
    """
    Describes a messaging broker interface.

    This interface abstracts functionality for sending and receiving messages,
    both asynchronously and synchronously, and for managing connection lifecycles.
    """

    async def aprocess_notify(
        self, connected_callback: Optional[Optional[Callable[[], Coroutine[Any, Any, None]]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:
        """The generator of messages from the broker for the dispatcherd service

        The producer iterates this to produce tasks.
        This uses the async connection of the broker.
        """
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        """Asynchronously send a message to the broker, used by dispatcherd service for reply messages"""
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

    def verify_self_check(self, message: dict[str, Any]) -> None:
        """Verify a received self check message"""
        ...


class ProducerEvents(Protocol):
    """
    Describes an events container for producers.

    Typically provides a signal (like a ready event) to indicate producer readiness.
    """

    ready_event: asyncio.Event
    recycle_event: asyncio.Event


class Producer(Protocol):
    """
    Describes a task producer interface.

    This interface encapsulates behavior for starting task production,
    managing its lifecycle, and tracking asynchronous operations.
    """

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

    async def recycle(self) -> None:
        """Restart the producer"""
        ...


class PoolWorker(Protocol):
    """
    Describes an individual worker in a task pool.

    It covers the properties and behaviors needed to track a worker’s execution state
    and control its task processing lifecycle.
    """

    current_task: Optional[dict]
    worker_id: int

    async def start_task(self, message: dict) -> None: ...

    def is_ready(self) -> bool: ...

    def get_data(self) -> dict[str, Any]:
        """Used for worker status control-and-reply command"""
        ...

    def cancel(self) -> None: ...


class Queuer(Protocol):
    """
    Describes an interface for managing pending tasks.

    It provides a way to iterate over and modify tasks awaiting assignment.
    """

    def __iter__(self) -> Iterator[dict]: ...

    def remove_task(self, message: dict) -> None: ...


class Blocker(Protocol):
    """
    Describes an interface for handling tasks that are temporarily deferred.

    It offers a mechanism to view and manage tasks that cannot run immediately.
    """

    def __iter__(self) -> Iterator[dict]: ...

    def remove_task(self, message: dict) -> None: ...


class WorkerData(Protocol):
    """
    Describes an interface for managing a collection of workers.

    It abstracts how worker instances are iterated over and retrieved,
    and it provides a lock for safe concurrent updates.
    """

    management_lock: asyncio.Lock

    def __iter__(self) -> Iterator[PoolWorker]: ...

    def get_by_id(self, worker_id: int) -> PoolWorker: ...


class WorkerPool(Protocol):
    """
    Describes an interface for a pool managing task workers.

    It includes core functionality for starting the pool, dispatching tasks,
    and shutting down the pool in a controlled manner.
    """

    workers: WorkerData
    queuer: Queuer
    blocker: Blocker

    async def start_working(self, forking_lock: asyncio.Lock, exit_event: Optional[asyncio.Event] = None) -> None:
        """Start persistent asyncio tasks, including asychronously starting worker subprocesses"""
        ...

    async def dispatch_task(self, message: dict) -> None:
        """Called by DispatcherMain after in the normal task lifecycle, pool will try to hand the task to a worker"""
        ...

    async def shutdown(self) -> None: ...


class DispatcherMain(Protocol):
    """
    Describes the primary dispatcherd interface.

    This interface defines the contract for the overall task dispatching service,
    including coordinating task processing, managing the worker pool, and
    handling delayed or control messages.
    """

    pool: WorkerPool
    delayed_messages: set

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
