import asyncio
import logging

from dispatcher.brokers.pg_notify import aget_connection, aprocess_notify, apublish_message

logger = logging.getLogger(__name__)


class BrokeredProducer:
    def __init__(self, broker='pg_notify', config=None, channels=()):
        self.production_task = None
        self.broker = broker
        self.config = config
        self.channels = channels
        self.connection = None

    async def start_producing(self, dispatcher):
        await self.connect()

        self.production_task = asyncio.create_task(self.produce_forever(dispatcher))
        # TODO: implement connection retry logic
        self.production_task.add_done_callback(dispatcher.fatal_error_callback)

    def all_tasks(self):
        if self.production_task:
            return [self.production_task]
        return []

    async def connect(self):
        self.connection = await aget_connection(self.config)

    async def produce_forever(self, dispatcher):
        async for channel, payload in aprocess_notify(self.connection, self.channels):
            logger.info(f"Received message from channel '{channel}': {payload}")
            await dispatcher.process_message(payload, broker=self)

    async def notify(self, channel, payload=None):
        await apublish_message(self.connection, channel, payload=payload)

    async def shutdown(self):
        if self.production_task:
            self.production_task.cancel()
            try:
                await self.production_task
            except asyncio.CancelledError:
                logger.info(f'Successfully canceled production from {self.broker}')
            except Exception:
                # traceback logged in fatal callback
                if not hasattr(self.production_task, '_dispatcher_tb_logged'):
                    logger.exception(f'Broker {self.broker} shutdown saw an unexpected exception from production task')
            self.production_task = None
        if self.connection:
            await self.connection.close()
            self.connection = None
