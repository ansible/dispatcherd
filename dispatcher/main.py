import asyncio
import json
import logging
import signal
from types import SimpleNamespace
from typing import Iterable, Optional

from dispatcher.pool import WorkerPool
from dispatcher.producers import BaseProducer

logger = logging.getLogger(__name__)


def task_filter_match(pool_task: dict, msg_data: dict) -> bool:
    """The two messages are functionally the same or not"""
    filterables = ('task', 'args', 'kwargs', 'uuid')
    for key in filterables:
        expected_value = msg_data.get(key)
        if expected_value:
            if pool_task.get(key, None) != expected_value:
                return False
    return True


async def _find_tasks(dispatcher, cancel: bool = False, **data) -> list[tuple[Optional[str], dict]]:
    "Utility method used for both running and cancel control methods"
    ret = []
    for worker in dispatcher.pool.workers.values():
        if worker.current_task:
            if task_filter_match(worker.current_task, data):
                if cancel:
                    logger.warning(f'Canceling task in worker {worker.worker_id}, task: {worker.current_task}')
                    worker.cancel()
                ret.append((worker.worker_id, worker.current_task))
    for message in dispatcher.pool.queued_messages:
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool queue: {message}')
                dispatcher.pool.queued_messages.remove(message)
            ret.append((None, message))
    for capsule in dispatcher.delayed_messages.copy():
        if task_filter_match(capsule.message, data):
            if cancel:
                logger.warning(f'Canceling delayed task (uuid={capsule.uuid})')
                capsule.task.cancel()
                try:
                    await capsule.task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.error(f'Error canceling delayed task (uuid={capsule.uuid})')
                dispatcher.delayed_messages.remove(capsule)
            ret.append(('<delayed>', capsule.message))
    return ret


class ControlTasks:
    # TODO: hold pool management lock for these things
    async def running(self, dispatcher, **data) -> list[tuple[Optional[str], dict]]:
        # TODO: include delayed tasks in results
        return await _find_tasks(dispatcher, **data)

    async def cancel(self, dispatcher, **data) -> list[tuple[Optional[str], dict]]:
        # TODO: include delayed tasks in results
        return await _find_tasks(dispatcher, cancel=True, **data)

    async def alive(self, dispatcher, **data):
        return


class DispatcherEvents:
    "Benchmark tests have to re-create this because they use same object in different event loops"

    def __init__(self) -> None:
        self.exit_event: asyncio.Event = asyncio.Event()


class DispatcherMain:
    def __init__(self, service_config: dict, producers: Iterable[BaseProducer]):
        self.delayed_messages: list[SimpleNamespace] = []
        self.received_count = 0
        self.control_count = 0
        self.ctl_tasks = ControlTasks()
        self.shutting_down = False
        # Lock for file descriptor mgmnt - hold lock when forking or connecting, to avoid DNS hangs
        # psycopg is well-behaved IFF you do not connect while forking, compare to AWX __clean_on_fork__
        self.fd_lock = asyncio.Lock()
        self.pool = WorkerPool(fd_lock=self.fd_lock, **service_config)

        # Set all the producers, this should still not start anything, just establishes objects
        self.producers = producers

        self.events: DispatcherEvents = DispatcherEvents()

    def _create_events(self):
        "Benchmark tests have to re-create this because they use same object in different event loops"
        return SimpleNamespace(exit_event=asyncio.Event())

    def fatal_error_callback(self, *args) -> None:
        """Method to connect to error callbacks of other tasks, will kick out of main loop"""
        if self.shutting_down:
            return

        for task in args:
            try:
                task.result()
            except Exception:
                logger.exception(f'Exception from task {task.get_name()}, exit flag set')
                task._dispatcher_tb_logged = True

        self.events.exit_event.set()

    def receive_signal(self, *args, **kwargs) -> None:
        logger.warning(f"Received exit signal args={args} kwargs={kwargs}")
        self.events.exit_event.set()

    async def wait_for_producers_ready(self) -> None:
        "Returns when all the producers have hit their ready event"
        for producer in self.producers:
            await producer.events.ready_event.wait()

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
        if self.delayed_messages:
            logger.debug('Shutting down delayed messages')
            for capsule in self.delayed_messages:
                capsule.task.cancel()
                try:
                    await capsule.task
                except asyncio.CancelledError:
                    logger.info(f'Canceled delayed task (uuid={capsule.uuid}) for shutdown')
                except Exception:
                    logger.exception(f'Error shutting down delayed task (uuid={capsule.uuid})')
            self.delayed_messages = []

        logger.debug('Gracefully shutting down worker pool')
        try:
            await self.pool.shutdown()
        except Exception:
            logger.exception('Pool manager encountered error')

        logger.debug('Setting event to exit main loop')
        self.events.exit_event.set()

    async def connected_callback(self, producer: BaseProducer) -> None:
        return

    async def sleep_then_process(self, capsule: SimpleNamespace) -> None:
        logger.info(f'Delaying {capsule.delay} s before running task: {capsule.message}')
        await asyncio.sleep(capsule.delay)
        logger.debug(f'Wakeup for delayed task: {capsule.message}')
        await self.process_message_internal(capsule.message)
        if capsule in self.delayed_messages:
            self.delayed_messages.remove(capsule)
            logger.info(f'fully processed delayed task (uuid={capsule.uuid})')

    def create_delayed_task(self, message: dict) -> None:
        "Called as alternative to sending to worker now, send to worker later"
        # capsule, as in, time capsule
        capsule = SimpleNamespace(uuid=message['uuid'], delay=message['delay'], message=message, task=None)
        new_task = asyncio.create_task(self.sleep_then_process(capsule))
        capsule.task = new_task
        self.delayed_messages.append(capsule)

    async def process_message(self, payload: dict, producer: Optional[BaseProducer] = None, channel: Optional[str] = None) -> None:
        # Convert payload from client into python dict
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
            return

        # A client may provide a task uuid (hope they do it correctly), if not add it
        if 'uuid' not in message:
            message['uuid'] = f'internal-{self.received_count}'
        if channel:
            message['channel'] = channel
        self.received_count += 1

        if 'delay' in message:
            # NOTE: control messages with reply should never be delayed, document this for users
            self.create_delayed_task(message)
        else:
            await self.process_message_internal(message, producer=producer)

    async def process_message_internal(self, message: dict, producer=None) -> None:
        if 'control' in message:
            method = getattr(self.ctl_tasks, message['control'])
            control_data = message.get('control_data', {})
            returned = await method(self, **control_data)
            if 'reply_to' in message:
                logger.info(f"Control action {message['control']} returned {returned}, sending via worker")
                self.control_count += 1
                await self.pool.dispatch_task(
                    {
                        'task': 'dispatcher.tasks.reply_to_control',
                        'args': [message['reply_to'], json.dumps(returned)],
                        'uuid': f'control-{self.control_count}',
                        'control': 'reply',  # for record keeping
                    }
                )
            else:
                logger.info(f"Control action {message['control']} returned {returned}, done")
        else:
            await self.pool.dispatch_task(message)

    async def start_working(self) -> None:
        logger.debug('Filling the worker pool')
        try:
            await self.pool.start_working(self)
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
        logger.info('Connecting dispatcher signal handling')
        await self.connect_signals()

        await self.start_working()

        logger.info('Dispatcher running forever, or until shutdown command')
        await self.events.exit_event.wait()

        await self.shutdown()

        await self.cancel_tasks()

        logger.debug('Dispatcher loop fully completed')
