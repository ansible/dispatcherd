import asyncio
import logging
import signal

from dispatcher.pool import WorkerPool
from dispatcher.producers.brokered import BrokeredProducer
from dispatcher.producers.scheduled import ScheduledProducer

logger = logging.getLogger(__name__)


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
            await producer.shutdown()

        logger.debug('Gracefully shutting down worker pool')
        await self.pool.shutdown()

        logger.debug('Setting event to exit main loop')
        self.exit_event.set()

    async def start_working(self):
        logger.debug('Filling the worker pool')
        await self.pool.start_working()

        logger.debug('Starting task production')
        for producer in self.producers:
            await producer.start_producing(self.pool)

    async def main(self):
        logger.info('Connecting dispatcher signal handling')
        await self.connect_signals()

        await self.start_working()

        logger.info('Dispatcher running forever, or until shutdown command')
        await self.exit_event.wait()

        logger.debug('Dispatcher loop fully completed')
