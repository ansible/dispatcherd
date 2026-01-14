import asyncio
import json
import logging
import signal
from os import getpid
from typing import Any, Iterable
from uuid import uuid4

from ..chunking import ChunkAccumulator
from ..processors.delayer import Delayer
from ..producers import BrokeredProducer
from ..protocols import Delayer as DelayerProtocol
from ..protocols import DispatcherMain as DispatcherMainProtocol
from ..protocols import DispatcherMetricsServer as DispatcherMetricsServerProtocol
from ..protocols import Producer
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from ..protocols import WorkerPool
from . import control_tasks
from .asyncio_tasks import cancel_other_tasks, ensure_fatal, wait_for_any

logger = logging.getLogger(__name__)


class DispatcherMain(DispatcherMainProtocol):
    def __init__(
        self,
        producers: Iterable[Producer],
        pool: WorkerPool,
        shared: SharedAsyncObjectsProtocol,
        node_id: str | None = None,
        metrics: DispatcherMetricsServerProtocol | None = None,
        chunk_message_timeout_seconds: float = 30 * 60,
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

        self.shutdown_lock = asyncio.Lock()

        self.metrics = metrics

        self.delayer: DelayerProtocol = Delayer(self.process_message_now, shared=shared)
        self.chunk_accumulator = ChunkAccumulator(
            message_timeout_seconds=chunk_message_timeout_seconds,
        )

    def receive_signal(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        logger.warning(f"Received exit signal args={args} kwargs={kwargs}")
        self.shared.exit_event.set()

    def get_status_data(self) -> dict[str, Any]:
        return {"received_count": self.received_count, "control_count": self.control_count, "pid": getpid()}

    async def connect_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.receive_signal)

    async def shutdown_no_lock(self) -> None:
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

        if self.metrics:
            try:
                await self.metrics.shutdown()
            except Exception:
                logger.exception('Metrics server shutdown encountered error')

        logger.debug('Setting event to exit main loop')
        self.shared.exit_event.set()

    async def shutdown(self) -> None:
        async with self.shutdown_lock:
            await self.shutdown_no_lock()

    async def connected_callback(self, producer: Producer) -> None:
        return

    async def process_message(self, payload: dict | str, producer: Producer | None = None, channel: str | None = None) -> tuple[str | None, str | None]:
        """Called by producers to trigger a new task

        Convert payload from producer into python dict
        Process uuid default
        Delay tasks when applicable
        Send to next layer of internal processing
        """
        if isinstance(payload, dict):
            decoded = payload
        elif isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except Exception:
                logger.warning('Received payload that is not valid JSON string; assuming bare task body')
                decoded = {'task': payload}
            if not isinstance(decoded, dict):
                logger.error('Decoded payload was not dict after json parsing')
                return (None, None)
        else:
            logger.error(f'Received unprocessable type {type(payload)}')
            return (None, None)

        is_chunk, assembled_message = await self.chunk_accumulator.aingest_dict(decoded)
        if is_chunk:
            if assembled_message is None:
                # Check for staleness, because failing to fully assemble now may be a give-up point
                await self.chunk_accumulator.aexpire_partial_messages()
                return (None, None)
            message = assembled_message
        else:
            message = decoded

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
            result = await self.process_message_now(immediate_message, producer=producer)
            # Piggyback on cleanup, expire partial messages that are too old
            await self.chunk_accumulator.aexpire_partial_messages()
            return result

        # We should be at this line if task was delayed, and in that case there is no reply message
        return (None, None)

    async def get_control_result(self, action: str, control_data: dict | None = None) -> dict:
        self.control_count += 1
        if (not hasattr(control_tasks, action)) or action.startswith('_'):
            logger.warning(f'Got invalid control request {action}, control_data: {control_data}')
            return {'error': f'No control method {action}'}
        else:
            method = getattr(control_tasks, action)
            if control_data is None:
                control_data = {}
            return await method(dispatcher=self, data=control_data)

    async def run_control_action(self, action: str, control_data: dict | None = None, reply_to: str | None = None) -> tuple[str | None, str | None]:
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

    async def process_message_now(self, message: dict, producer: Producer | None = None) -> tuple[str | None, str | None]:
        """Route message to control action or to a worker via the pool. Does not consider task delays."""
        if 'control' in message:
            return await self.run_control_action(message['control'], control_data=message.get('control_data'), reply_to=message.get('reply_to'))
        else:
            await self.pool.dispatch_task(message)
        return (None, None)

    async def start_working(self) -> None:
        logger.debug('Filling the worker pool')
        self.shared.exit_event.clear()

        if self.metrics:
            metrics_task = await self.metrics.start_working(self)
            if metrics_task:
                ensure_fatal(metrics_task, exit_event=self.shared.exit_event)

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

    async def main(self) -> None:
        """Main method for the event loop, intended to be passed to loop.run_until_complete"""
        current_task = asyncio.current_task()
        if current_task is not None:
            current_task.set_name('dispatcherd_service_main')

        await self.connect_signals()
        try:
            await self.main_as_task()
        finally:
            await cancel_other_tasks()

        logger.debug('Dispatcherd loop fully completed')
