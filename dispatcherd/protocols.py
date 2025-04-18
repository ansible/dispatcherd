import asyncio
import multiprocessing
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
    ) -> AsyncGenerator[tuple[Union[int, str], str], None]:
        """The generator of messages from the broker for the dispatcherd service

        The producer iterates this to produce tasks.
        This uses the async connection of the broker.
        """
        yield ('', '')  # yield affects CPython type https://github.com/python/mypy/pull/18422

    async def apublish_message(self, channel: Optional[str] = None, origin: Union[int, str, None] = None, message: str = '') -> None:
        """Asynchronously send a message to the broker, used by dispatcherd service for reply messages"""
        ...

    async def aclose(self) -> None:
        """Close the asynchronous connection, used by service, and optionally by publishers"""
        ...

    def process_notify(
        self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1
    ) -> Iterator[tuple[Union[int, str], str]]:
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
    can_recycle: bool

    async def start_producing(self, dispatcher: 'DispatcherMain') -> None:
        """Starts tasks which will eventually call DispatcherMain.process_message - how tasks originate in the service"""
        ...

    def get_status_data(self) -> dict:
        """Data for debugging commands"""
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

    @property
    def is_ready(self) -> bool: ...

    @property
    def counts_for_capacity(self) -> bool: ...

    @property
    def expected_alive(self) -> bool: ...

    def get_status_data(self) -> dict[str, Any]:
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


class DelayCapsule(Protocol):
    """Task wrapper for delayed tasks"""

    delay: float
    message: dict
    has_ran: bool


class Delayer(Protocol):
    """
    Describes an interface for handling tasks that specifify a delay before running.

    This runs an asyncio task as needed to get wakeups.
    It advances tasks into the next stage of processing by the callback passed in.
    """

    def __iter__(self) -> Iterator[DelayCapsule]: ...

    async def process_task(self, message: dict) -> Optional[dict]: ...

    def remove_capsule(self, capsule: DelayCapsule) -> None: ...

    async def shutdown(self) -> None: ...


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


class SharedAsyncObjects:
    exit_event: asyncio.Event
    forking_and_connecting_lock: asyncio.Lock  # Forking and locking may need to be serialized, which this does


class PoolEvents:
    """
    Container for events related to the WorkerPool

    These have so far been events triggered _by_ the worker pool.
    This is useful for writing tests or 3rd party customization of the main loop.
    """

    queue_cleared: asyncio.Event
    work_cleared: asyncio.Event = asyncio.Event()
    management_event: asyncio.Event = asyncio.Event()
    workers_ready: asyncio.Event = asyncio.Event()


class WorkerPool(Protocol):
    """
    Describes an interface for a pool managing task workers.

    It includes core functionality for starting the pool, dispatching tasks,
    and shutting down the pool in a controlled manner.
    """

    workers: WorkerData
    queuer: Queuer
    blocker: Blocker
    shared: SharedAsyncObjects
    events: PoolEvents
    finished_count: int

    async def start_working(self, dispatcher: 'DispatcherMain') -> None:
        """Start persistent asyncio tasks, including asychronously starting worker subprocesses"""
        ...

    async def dispatch_task(self, message: dict) -> None:
        """Called by DispatcherMain after in the normal task lifecycle, pool will try to hand the task to a worker"""
        ...

    def get_status_data(self) -> dict:
        """Data for debugging commands"""
        ...

    async def shutdown(self) -> None: ...


class DispatcherMetricsServer(Protocol):
    """
    Metrics object will be created by factories.

    If the extra is not installed, we may not have needed python dependencies.
    This only needs to capture the interace of the initialized object with DispatcherMain.
    """

    async def start_server(self, dispatcher: 'DispatcherMain') -> None: ...


class DispatcherMain(Protocol):
    """
    Describes the primary dispatcherd interface.

    This interface defines the contract for the overall task dispatching service,
    including coordinating task processing, managing the worker pool, and
    handling delayed or control messages.
    """

    pool: WorkerPool
    shared: SharedAsyncObjects
    producers: Iterable[Producer]
    delayer: Delayer

    received_count: int
    control_count: int

    async def main(self) -> None:
        """This is the method that runs the service, bring your own event loop"""
        ...

    async def connected_callback(self, producer: Producer) -> None:
        """Called by producers when they are connected"""
        ...

    async def get_control_result(self, action: str, control_data: Optional[dict] = None) -> dict:
        """Used by WorkerPool if a task needs to run a local control task"""
        ...

    async def process_message(
        self, payload: Union[dict, str], producer: Optional[Producer] = None, channel: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        """This is called by producers when a new request to run a task comes in"""
        ...

    def get_status_data(self) -> dict:
        """Data for debugging commands"""
        ...


class TaskWorker(Protocol):
    """
    Object used in the synchronous workers, interacts with the work_loop

    contains callbacks offered for the user to override
    """

    worker_id: int
    idle_timeout: int
    finished_queue: multiprocessing.Queue
    message_queue: multiprocessing.Queue

    def __init__(self, worker_id: int, message_queue: multiprocessing.Queue, finished_queue: multiprocessing.Queue, **kwargs) -> None: ...

    def should_exit(self) -> bool: ...

    def on_start(self) -> None: ...

    def on_shutdown(self) -> None: ...

    def pre_task(self, message: dict) -> None: ...

    def post_task(self, result: dict) -> None: ...

    def on_idle(self) -> None: ...

    def perform_work(self, message: dict) -> dict: ...

    def get_ready_message(self) -> dict[str, Union[str, int]]: ...

    def get_shutdown_message(self) -> dict[str, Union[str, int]]: ...
