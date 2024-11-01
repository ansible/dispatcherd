import asyncio
import logging
import signal
import json
from types import SimpleNamespace

from dispatcher.pool import WorkerPool
from dispatcher.producers.brokered import BrokeredProducer
from dispatcher.producers.scheduled import ScheduledProducer

logger = logging.getLogger(__name__)


def task_filter_match(pool_task, msg_data):
    filterables = ('task', 'args', 'kwargs', 'uuid')
    for key in filterables:
        expected_value = msg_data.get(key)
        if expected_value:
            if pool_task.get(key, None) != expected_value:
                return False
    return True


async def _find_tasks(dispatcher, cancel=False, **data):
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
    async def running(self, dispatcher, **data):
        # TODO: include delayed tasks in results
        return await _find_tasks(dispatcher, **data)

    async def cancel(self, dispatcher, **data):
        # TODO: include delayed tasks in results
        return await _find_tasks(dispatcher, cancel=True, **data)

    async def alive(self, dispatcher, **data):
        return


class DispatcherMain:
    def __init__(self, config):
        self.exit_event = asyncio.Event()
        num_workers = 3
        self.pool = WorkerPool(num_workers)
        self.delayed_messages = []
        self.received_count = 0
        self.ctl_tasks = ControlTasks()
        self.shutting_down = False

        # Initialize all the producers, this should not start anything, just establishes objects
        self.producers = []
        if 'producers' in config:
            producer_config = config['producers']
            if 'brokers' in producer_config:
                for broker_name, broker_config in producer_config['brokers'].items():
                    # TODO: import from the broker module here, some importlib stuff
                    # TODO: make channels specific to broker, probably
                    if broker_name != 'pg_notify':
                        continue
                    self.producers.append(BrokeredProducer(broker=broker_name, config=broker_config, channels=producer_config['brokers']['channels']))
            if 'scheduled' in producer_config:
                self.producers.append(ScheduledProducer(producer_config['scheduled']))

    def fatal_error_callback(self, *args):
        if self.shutting_down:
            return

        for task in args:
            try:
                task.result()
            except Exception:
                logger.exception(f'Exception from {task.get_name()}, exit flag set')
                task._dispatcher_tb_logged = True

        self.exit_event.set()

    def receive_signal(self, sig=None):
        if sig:
            logging.warning(f"Received exit signal {sig.name}...")
        self.exit_event.set()

    async def connect_signals(self):
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.receive_signal)

    async def shutdown(self):
        self.shutting_down = True
        logging.debug("Shutting down, starting with producers.")
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
        self.exit_event.set()

    async def sleep_then_process(self, capsule):
        logger.info(f'Delaying {capsule.delay} s before running task: {capsule.message}')
        await asyncio.sleep(capsule.delay)
        logger.debug(f'Wakeup for delayed task: {capsule.message}')
        await self.process_message_internal(capsule.message)
        if capsule in self.delayed_messages:
            self.delayed_messages.remove(capsule)
            logger.info(f'fully processed delayed task (uuid={capsule.uuid})')

    def create_delayed_task(self, message):
        "Called as alternative to sending to worker now, send to worker later"
        # capsule, as in, time capsule
        capsule = SimpleNamespace(
            uuid=message['uuid'],
            delay=message['delay'],
            message=message,
            task=None
        )
        new_task = asyncio.create_task(self.sleep_then_process(capsule))
        capsule.task = new_task
        self.delayed_messages.append(capsule)

    async def process_message(self, payload, broker=None):
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
        self.received_count += 1

        if 'delay' in message:
            # NOTE: control messages with reply should never be delayed, document this for users
            self.create_delayed_task(message)
        else:
            await self.process_message_internal(message, broker=broker)

    async def process_message_internal(self, message, broker=None):
        if 'control' in message:
            method = getattr(self.ctl_tasks, message['control'])
            control_data = message.get('control_data', {})
            returned = await method(self, **control_data)
            if 'reply_to' in message:
                logger.info(f"Control action {message['control']} returned {returned}, sending via worker")
                await self.pool.dispatch_task({
                    'task': 'dispatcher.brokers.pg_notify.publish_message',
                    'args': [message['reply_to'], json.dumps(returned)],
                    'kwargs': {'config': broker.config}
                })
            else:
                logger.info(f"Control action {message['control']} returned {returned}, done")
        else:
            await self.pool.dispatch_task(message)

    async def start_working(self):
        logger.debug('Filling the worker pool')
        await self.pool.start_working(self)

        logger.debug('Starting task production')
        for producer in self.producers:
            await producer.start_producing(self)

    async def main(self):
        logger.info('Connecting dispatcher signal handling')
        await self.connect_signals()

        await self.start_working()

        logger.info('Dispatcher running forever, or until shutdown command')
        await self.exit_event.wait()

        await self.shutdown()

        logger.debug('Dispatcher loop fully completed')
