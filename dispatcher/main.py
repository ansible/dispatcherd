import asyncio
import logging
import signal
import json

from dispatcher.pool import WorkerPool
from dispatcher.producers.brokered import BrokeredProducer
from dispatcher.producers.scheduled import ScheduledProducer

logger = logging.getLogger(__name__)


class ControlTasks:
    def running(self, dispatcher, task=None, args=None):
        ret = []
        for worker in dispatcher.pool.workers.values():
            if worker.current_task:
                if task and worker.current_task['task'] != task:
                    continue
                if args and worker.current_task['args'] != args:
                    continue
                ret.append((worker.worker_id, worker.current_task))
        for message in dispatcher.pool.queued_messages:
            if task and message['task'] != task:
                continue
            if args and message['args'] != args:
                continue
            ret.append((None, message))
        return ret


class DispatcherMain:
    def __init__(self, config):
        self.exit_event = asyncio.Event()
        num_workers = 3
        self.pool = WorkerPool(num_workers)

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

        logging.debug(f"Shutting down, starting with producers.")
        for producer in self.producers:
            try:
                await producer.shutdown()
            except Exception:
                logger.exception('Producer task had error')

        logger.debug('Gracefully shutting down worker pool')
        try:
            await self.pool.shutdown()
        except Exception:
            logger.exception('Pool manager encountered error')

        logger.debug('Setting event to exit main loop')
        self.exit_event.set()

    async def process_message(self, payload):
        # TODO: handle this more elegantly, or tell clients not to do this
        if isinstance(payload, str):
            try:
                message = json.loads(payload)
            except Exception:
                message = {'task': payload}

        if 'control' in message:
            logger.info(f'Processing control message in main {message}')
            ctl_tasks = ControlTasks()
            method = getattr(ctl_tasks, message['control'])
            args = message.get('args', [])
            kwargs = message.get('kwargs', {})
            returned = method(self, *args, **kwargs)
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
