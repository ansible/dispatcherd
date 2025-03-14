import asyncio
import json
import logging
import signal
import time
from typing import Iterable, Optional, Union
from uuid import uuid4

from ..brokers import get_broker
from ..producers import BrokeredProducer
from ..protocols import Producer, BrokerSelfCheckResult
from . import control_tasks
from .asyncio_tasks import ensure_fatal
from .next_wakeup_runner import HasWakeup, NextWakeupRunner
from .pool import WorkerPool

logger = logging.getLogger(__name__)


class DispatcherEvents:
    "Benchmark tests have to re-create this because they use same object in different event loops"

    def __init__(self) -> None:
        self.exit_event: asyncio.Event = asyncio.Event()


class DelayCapsule(HasWakeup):
    """When a task has a delay, this tracks the delay"""

    def __init__(self, delay: float, message: dict) -> None:
        self.has_ran: bool = False
        self.received_at = time.monotonic()
        self.delay = delay
        self.message = message

    def next_wakeup(self) -> Optional[float]:
        if self.has_ran is True:
            return None
        return self.received_at + self.delay


class DispatcherMain:
    def __init__(self, producers: Iterable[Producer], pool: WorkerPool, node_id: Optional[str] = None):
        self.delayed_messages: set[DelayCapsule] = set()
        self.received_count = 0
        self.control_count = 0
        self.shutting_down = False
        # Lock for file descriptor mgmnt - hold lock when forking or connecting, to avoid DNS hangs
        # psycopg is well-behaved IFF you do not connect while forking, compare to AWX __clean_on_fork__
        self.fd_lock = asyncio.Lock()

        # Save the associated dispatcher objects, usually created by factories
        # expected that these are not yet running any tasks
        self.pool = pool
        self.producers = producers

        # Identifer for this instance of the dispatcher service, sent in reply messages
        if node_id:
            self.node_id = node_id
        else:
            self.node_id = str(uuid4())

        self.events: DispatcherEvents = DispatcherEvents()

        self.delayed_runner = NextWakeupRunner(self.delayed_messages, self.process_delayed_task, name='delayed_task_runner')
        self.delayed_runner.exit_event = self.events.exit_event

    def receive_signal(self, *args, **kwargs) -> None:
        logger.warning(f"Received exit signal args={args} kwargs={kwargs}")
        self.events.exit_event.set()

    async def wait_for_producers_ready(self, timeout=3) -> None:
        "Returns when all the producers have hit their ready event"
        for producer in self.producers:
            existing_tasks = list(producer.all_tasks())
            wait_task = asyncio.create_task(producer.events.ready_event.wait(), name=f'tmp_{producer}_wait_task')
            existing_tasks.append(wait_task)
            await asyncio.wait(existing_tasks, return_when=asyncio.FIRST_COMPLETED)
            if not wait_task.done():
                producer.events.ready_event.set()  # exits wait_task, producer had error

    async def connect_signals(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.receive_signal)

    async def shutdown(self) -> None:
        self.shutting_down = True
        logger.debug("Shutting down, starting with producers.")
        for producer in self.producers:
            try:
                await producer.shutdown()
            except Exception:
                logger.exception('Producer task had error')

        # Handle delayed tasks and inform user
        await self.delayed_runner.shutdown()
        for capsule in self.delayed_messages:
            logger.warning(f'Abandoning delayed task (due to shutdown) to run in {capsule.delay}, message={capsule.message}')
        self.delayed_messages = set()

        logger.debug('Gracefully shutting down worker pool')
        try:
            await self.pool.shutdown()
        except Exception:
            logger.exception('Pool manager encountered error')

        logger.debug('Setting event to exit main loop')
        self.events.exit_event.set()

    async def connected_callback(self, producer: Producer) -> None:
        return

    async def process_delayed_task(self, capsule: DelayCapsule) -> None:
        capsule.has_ran = True
        logger.debug(f'Wakeup for delayed task: {capsule.message}')
        await self.process_message_internal(capsule.message)
        self.delayed_messages.remove(capsule)

    async def create_delayed_task(self, message: dict) -> None:
        "Called as alternative to sending to worker now, send to worker later"
        # capsule, as in, time capsule
        capsule = DelayCapsule(message['delay'], message)
        logger.info(f'Delaying {capsule.delay} s before running task: {capsule.message}')
        self.delayed_messages.add(capsule)
        await self.delayed_runner.kick()

    async def process_message(
        self, payload: Union[dict, str], producer: Optional[Producer] = None, channel: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        """Called by producers to trigger a new task

        Convert payload from producer into python dict
        Process uuid default
        Delay tasks when applicable
        Send to next layer of internal processing
        """
        # TODO: more structured validation of the incoming payload from publishers
        if isinstance(payload, str):
            try:
                message = json.loads(payload)
            except Exception:
                message = {'task': payload}
        elif isinstance(payload, dict):
            message = payload
        else:
            logger.error(f'Received unprocessable type {type(payload)}')
            return (None, None)

        # A client may provide a task uuid (hope they do it correctly), if not add it
        if 'uuid' not in message:
            message['uuid'] = f'internal-{self.received_count}'
        if channel:
            message['channel'] = channel
        self.received_count += 1

        if 'delay' in message:
            # NOTE: control messages with reply should never be delayed, document this for users
            await self.create_delayed_task(message)
        else:
            return await self.process_message_internal(message, producer=producer)
        return (None, None)

    async def run_control_action(self, action: str, control_data: Optional[dict] = None, reply_to: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        return_data = {}

        # Get the result
        if (not hasattr(control_tasks, action)) or action.startswith('_'):
            logger.warning(f'Got invalid control request {action}, control_data: {control_data}, reply_to: {reply_to}')
            if reply_to:
                return_data = {'error': f'No control method {action}'}
        else:
            method = getattr(control_tasks, action)
            if control_data is None:
                control_data = {}
            return_data = await method(self, **control_data)

        # Identify the current node in the response
        return_data['node_id'] = self.node_id
        self.control_count += 1

        # Give Nones for no reply, or the reply
        if reply_to:
            logger.info(f"Control action {action} returned {return_data}, sending back reply")
            return (reply_to, json.dumps(return_data))
        else:
            logger.info(f"Control action {action} returned {return_data}, done")
            return (None, None)

    async def process_message_internal(self, message: dict, producer=None) -> tuple[Optional[str], Optional[str]]:
        """Route message based on needed action - delay for later, return reply, or dispatch to worker"""
        if 'control' in message:
            return await self.run_control_action(message['control'], control_data=message.get('control_data'), reply_to=message.get('reply_to'))
        else:
            await self.pool.dispatch_task(message)
        return (None, None)

    async def start_working(self) -> None:
        logger.debug('Filling the worker pool')
        try:
            await self.pool.start_working(self.fd_lock, exit_event=self.events.exit_event)
        except Exception:
            logger.exception(f'Pool {self.pool} failed to start working')
            self.events.exit_event.set()

        logger.debug('Starting task production')
        async with self.fd_lock:  # lots of connecting going on here
            for producer in self.producers:
                try:
                    await producer.start_producing(self)
                except Exception:
                    logger.exception(f'Producer {producer} failed to start')
                    self.events.exit_event.set()

                # TODO: recycle producer instead of raising up error
                # https://github.com/ansible/dispatcherd/issues/2
                for task in producer.all_tasks():
                    ensure_fatal(task, exit_event=self.events.exit_event)

    async def cancel_tasks(self):
        for task in asyncio.all_tasks():
            if task == asyncio.current_task():
                continue
            if not task.done():
                logger.warning(f'Task {task} did not shut down in shutdown method')
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def main(self) -> None:
        await self.connect_signals()

        try:
            await self.start_working()

            logger.info(f'Dispatcher node_id={self.node_id} running forever, or until shutdown command')

            while True:
                try:
                    await asyncio.wait_for(self.events.exit_event.wait(), 5.0)

                    # exit_event has fired, process should exit
                    break
                except asyncio.TimeoutError:
                    logger.info(f'initiating broker self check for node-id {self.node_id}')
                    for producer in self.producers:
                        if isinstance(producer, BrokeredProducer):
                            result = await producer.broker.get_self_check_result(self.node_id)

                            if result == BrokerSelfCheckResult.IN_PROGRESS:
                                # the last self check still hasn't finished - we treat that as a connection failure
                                result = BrokerSelfCheckResult.FAILURE

                            match result:
                                case BrokerSelfCheckResult.UNDECIDED | BrokerSelfCheckResult.SUCCESS:
                                    asyncio.create_task(producer.broker.initiate_self_check(self.node_id))
                                    continue
                                case BrokerSelfCheckResult.FAILURE:
                                    logger.error(f'broker self check failed for node-id {self.node_id}')
                                    producer.broker.reconnect()

        finally:
            await self.shutdown()

            await self.cancel_tasks()

        logger.debug('Dispatcher loop fully completed')
