import asyncio
import logging

from dispatcher.brokers.pg_notify import aget_connection, aprocess_notify

logger = logging.getLogger(__name__)


class BrokeredProducer:
    def __init__(self, broker='pg_notify', config=None, channels=()):
        self.production_task = None
        self.broker = broker
        self.config = config
        self.channels = channels

    async def start_producing(self, pool):
        self.production_task = asyncio.create_task(self.produce_forever(pool))

    def all_tasks(self):
        if self.production_task:
            return [self.production_task]
        return []

    async def connect(self):
        self.connection = await aget_connection(self.config)

    async def produce_forever(self, pool):
        await self.connect()

        async with self.connection:

            async for channel, payload in aprocess_notify(self.connection, self.channels):
                logger.info(f"Received message from channel '{channel}': {payload}, sending to worker")
                await pool.dispatch_task(payload)

    async def shutdown(self):
        if self.production_task:
            self.production_task.cancel()
            try:
                await self.production_task
            except asyncio.CancelledError:
                logger.info(f'Successfully canceled production from {self.broker}')
            self.production_task = None
        if self.connection:
            await self.connection.close()
            self.connection = None
