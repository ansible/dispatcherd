import asyncio
import json
import logging
import signal
from os import getpid
from typing import Any, Iterable, Optional, Union
from uuid import uuid4

from ..producers import BrokeredProducer
from ..protocols import Delayer as DelayerProtocol
from ..protocols import DispatcherMain as DispatcherMainProtocol
from ..protocols import DispatcherMetricsServer as DispatcherMetricsServerProtocol
from ..protocols import Producer
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from ..protocols import WorkerPool
from . import control_tasks
from .asyncio_tasks import ensure_fatal, wait_for_any
from .delayer import Delayer

logger = logging.getLogger(__name__)


class DispatcherMain(DispatcherMainProtocol):
    def __init__(
        self,
        producers: Iterable[Producer],
        pool: WorkerPool,
        shared: SharedAsyncObjectsProtocol,
        node_id: Optional[str] = None,
        metrics: Optional[DispatcherMetricsServerProtocol] = None,
    ):
        self.received_count = 0
        self.control_count = 0

        # Save the associated dispatcher objects, usually created by factories
        # expected that these are not yet running any tasks
        self.pool = pool
        self.producers = producers
        self.shared = shared

        # Identifer for this instance of the dispatcherd service, sent in reply messages
        if node_id:
            self.node_id = node_id
        else:
            self.node_id = str(uuid4())

        self.metrics = metrics

        self.delayer: DelayerProtocol = Delayer(self.process_message_now, shared=shared)

    def receive_signal(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.warning(f"Received exit signal args={args} kwargs={kwargs}")
        self.shared.exit_event.set()

    def get_status_data(self) -> dict[str, Any]:
        return {"received_count": self.received_count, "control_count": self.control_count, "pid": getpid()}

    async def wait_for_producers_ready(self) -> None:
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
        self.shared.exit_event.set()  # may already be set
        logger.debug("Shutting down, starting with producers.")
        for producer in self.producers:
            try:
                await producer.shutdown()
            except Exception:
                logger.exception('Producer task had error')

        # Handle delayed tasks and inform user
        await self.delayer.shutdown()

        logger.debug('Gracefully shutting down worker pool')
        try:
            await self.pool.shutdown()
        except Exception:
            logger.exception('Pool manager encountered error')

        logger.debug('Setting event to exit main loop')
        self.shared.exit_event.set()

    async def connected_callback(self, producer: Producer) -> None:
        return

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

        if 'self_check' in message:
            if isinstance(producer, BrokeredProducer):
                producer.broker.verify_self_check(message)

        # A client may provide a task uuid (hope they do it correctly), if not add it
        if 'uuid' not in message:
            message['uuid'] = f'internal-{self.received_count}'
        if channel:
            message['channel'] = channel
        self.received_count += 1

        if immediate_message := await self.delayer.process_task(message):
            return await self.process_message_now(immediate_message, producer=producer)

        # We should be at this line if task was delayed, and in that case there is no reply message
        return (None, None)

    async def get_control_result(self, action: str, control_data: Optional[dict] = None) -> dict:
        self.control_count += 1
        if (not hasattr(control_tasks, action)) or action.startswith('_'):
            logger.warning(f'Got invalid control request {action}, control_data: {control_data}')
            return {'error': f'No control method {action}'}
        else:
            method = getattr(control_tasks, action)
            if control_data is None:
                control_data = {}
            return await method(dispatcher=self, data=control_data)

    async def run_control_action(self, action: str, control_data: Optional[dict] = None, reply_to: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        return_data = {}

        # Get the result
        return_data = await self.get_control_result(action=action, control_data=control_data)

        # Identify the current node in the response
        return_data['node_id'] = self.node_id

        # Give Nones for no reply, or the reply
        if reply_to:
            reply_msg = json.dumps(return_data)
            logger.info(f"Control action {action} returned message len={len(reply_msg)}, sending back reply")
            return (reply_to, reply_msg)
        else:
            logger.info(f"Control action {action} returned {type(return_data)}, done")
            return (None, None)

    async def process_message_now(self, message: dict, producer: Optional[Producer] = None) -> tuple[Optional[str], Optional[str]]:
        """Route message to control action or to a worker via the pool. Does not consider task delays."""
        if 'control' in message:
            return await self.run_control_action(message['control'], control_data=message.get('control_data'), reply_to=message.get('reply_to'))
        else:
            await self.pool.dispatch_task(message)
        return (None, None)

    async def start_working(self) -> None:
        logger.debug('Filling the worker pool')
        try:
            await self.pool.start_working(self)
        except Exception:
            logger.exception(f'Pool {self.pool} failed to start working')
            self.shared.exit_event.set()

        async with self.shared.forking_and_connecting_lock:  # lots of connecting going on here
            for producer in self.producers:
                logger.debug(f'Starting task production from {producer}')
                try:
                    await producer.start_producing(self)
                except Exception:
                    logger.exception(f'Producer {producer} failed to start')
                    producer.events.recycle_event.set()

                # TODO: recycle producer instead of raising up error
                # https://github.com/ansible/dispatcherd/issues/2
                for task in producer.all_tasks():
                    ensure_fatal(task, exit_event=producer.events.recycle_event)

    async def cancel_tasks(self) -> None:
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

    async def recycle_broker_producers(self) -> None:
        """For any producer in a broken state (likely due to external factors beyond our control) recycle it"""
        for producer in self.producers:
            if not producer.can_recycle:
                continue
            if producer.events.recycle_event.is_set():
                await producer.recycle()
                for task in producer.all_tasks():
                    ensure_fatal(task, exit_event=producer.events.recycle_event)
                logger.info('finished recycling of producer')

    async def main_loop_wait(self) -> None:
        """Wait for an event that requires some kind of action by the main loop"""
        events = [self.shared.exit_event]
        names = ['exit_event_wait']
        for producer in self.producers:
            if not producer.can_recycle:
                continue
            events.append(producer.events.recycle_event)
            names.append(f'{str(producer)}_recycle_event_wait')

        await wait_for_any(events, names=names)

    async def main_as_task(self) -> None:
        """This should be called for the main loop if running as part of another asyncio program"""
        metrics_task: Optional[asyncio.Task] = None
        if self.metrics:
            metrics_task = asyncio.create_task(self.metrics.start_server(self), name='metrics_server')
            ensure_fatal(metrics_task, exit_event=self.shared.exit_event)

        try:
            await self.start_working()

            logger.info(f'Dispatcherd node_id={self.node_id} running forever, or until shutdown command')

            while True:
                await self.main_loop_wait()

                if self.shared.exit_event.is_set():
                    break  # If the exit event is set, terminate the process
                else:
                    await self.recycle_broker_producers()  # Otherwise, one or some of the producers broke

        finally:
            await self.shutdown()

            if metrics_task:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    logger.debug('Metrics server has been canceled')

    async def main(self) -> None:
        """Main method for the event loop, intended to be passed to loop.run_until_complete"""
        current_task = asyncio.current_task()
        if current_task is not None:
            current_task.set_name('dispatcherd_service_main')

        await self.connect_signals()
        try:
            await self.main_as_task()
        finally:
            await self.cancel_tasks()

        logger.debug('Dispatcherd loop fully completed')
