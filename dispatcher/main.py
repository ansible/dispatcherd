import asyncio
import logging
import signal
import json

from dispatcher.pool import WorkerPool
from dispatcher.producers.brokered import BrokeredProducer
from dispatcher.producers.scheduled import ScheduledProducer

logger = logging.getLogger(__name__)


def task_filter_match(pool_task, msg_data):
    filterables = ('task', 'args', 'kwargs', 'uuid')
    for key in filterables:
        if key in msg_data:
            if pool_task.get(key, None) != msg_data[key]:
                return False
    return True


def _find_tasks(dispatcher, cancel=False, **data):
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
    return ret


class ControlTasks:
    # TODO: hold pool management lock for these things
    def running(self, dispatcher, **data):
        # TODO: include delayed tasks in results
        return _find_tasks(dispatcher, **data)

    def cancel(self, dispatcher, **data):
        # TODO: include delayed tasks in results
        return _find_tasks(dispatcher, cancel=True, **data)

    def alive(self, dispatcher, **data):
        return


class DispatcherMain:
    def __init__(self, config):
        self.exit_event = asyncio.Event()
        num_workers = 3
        self.pool = WorkerPool(num_workers)
        self.delayed_messages = []

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

    async def connect_signals(self):
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown(sig)))

    async def shutdown(self, sig=None):
        if sig:
            logging.info(f"Received exit signal {sig.name}...")

        logging.debug("Shutting down, starting with producers.")
        for producer in self.producers:
            try:
                await producer.shutdown()
            except Exception:
                logger.exception('Producer task had error')
        if self.delayed_messages:
            logger.debug('Shutting down delayed messages')
            for delayed_task in self.delayed_messages:
                delayed_task.cancel()
                try:
                    await delayed_task
                except asyncio.CancelledError:
                    logger.debug(f'Successfully canceled delayed task {delayed_task}')
                except Exception:
                    logger.exception(f'Error shutting down delayed task {delayed_task}')
            self.delayed_messages = []

        logger.debug('Gracefully shutting down worker pool')
        try:
            await self.pool.shutdown()
        except Exception:
            logger.exception('Pool manager encountered error')

        logger.debug('Setting event to exit main loop')
        self.exit_event.set()

    async def delay_message(self, message, delay):
        logger.info(f'Delaying {delay} s before running task: {message}')
        await asyncio.sleep(delay)
        logger.debug(f'Wakeup for delayed task: {message}')
        await self.process_message(message, allow_delay=False)
        task = asyncio.current_task()
        if task in self.delayed_messages:
            self.delayed_messages.remove(task)
            logger.info(f'fully processed delayed task {message}')

    async def process_message(self, payload, allow_delay=True):
        # TODO: handle this more elegantly, or tell clients not to do this
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

        if allow_delay and 'delay' in message:
            self.delayed_messages.append(asyncio.create_task(self.delay_message(message, message['delay'])))
            return

        if 'control' in message:
            logger.info(f'Processing control message in main {message}')
            ctl_tasks = ControlTasks()
            method = getattr(ctl_tasks, message['control'])
            returned = method(self, **message)
            return_info = {'result': json.dumps(returned)}
            logger.info(f'Prepared reply data {return_info["result"]}')
            if 'reply_to' in message:
                return_info['reply_to'] = message['reply_to']
            return return_info
        else:
            await self.pool.dispatch_task(message)

    async def start_working(self):
        logger.debug('Filling the worker pool')
        await self.pool.start_working()

        logger.debug('Starting task production')
        for producer in self.producers:
            await producer.start_producing(self)

    async def main(self):
        logger.info('Connecting dispatcher signal handling')
        await self.connect_signals()

        await self.start_working()

        logger.info('Dispatcher running forever, or until shutdown command')
        await self.exit_event.wait()

        logger.debug('Dispatcher loop fully completed')
