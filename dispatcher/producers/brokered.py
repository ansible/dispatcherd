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

    async def start_producing(self, dispatcher):
        self.production_task = asyncio.create_task(self.produce_forever(dispatcher))

    def all_tasks(self):
        if self.production_task:
            return [self.production_task]
        return []

    async def connect(self):
        self.connection = await aget_connection(self.config)

    async def produce_forever(self, dispatcher):
        await self.connect()

        async with self.connection as acur:

            async for channel, payload in aprocess_notify(self.connection, self.channels):
                logger.info(f"Received message from channel '{channel}': {payload}, sending to worker")
                reply = await dispatcher.process_message(payload)
                if reply and 'reply_to' in reply:
                    logger.info(f'Sending result {reply["result"]} control reply to channel {reply["reply_to"]}')
                    await acur.execute('SELECT pg_notify(%s, %s);', (reply['reply_to'], reply['result']))

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
