import asyncio
import logging
from typing import Optional

from dispatcher.brokers.pg_notify import aget_connection, aprocess_notify, apublish_message
from dispatcher.producers.base import BaseProducer

logger = logging.getLogger(__name__)


class BrokeredProducer(BaseProducer):
    def __init__(self, broker: str = 'pg_notify', config: Optional[dict] = None, channels: tuple = (), connection=None) -> None:
        self.events = self._create_events()
        self.production_task: Optional[asyncio.Task] = None
        self.broker = broker
        self.config = config
        self.channels = channels
        self.connection = connection
        self.old_connection = bool(connection)

    async def start_producing(self, dispatcher) -> None:
        await self.connect()

        self.production_task = asyncio.create_task(self.produce_forever(dispatcher))
        # TODO: implement connection retry logic
        self.production_task.add_done_callback(dispatcher.fatal_error_callback)

    def all_tasks(self) -> list[asyncio.Task]:
        if self.production_task:
            return [self.production_task]
        return []

    async def connect(self):
        if self.connection is None:
            self.connection = await aget_connection(self.config)

    async def produce_forever(self, dispatcher) -> None:
        async for channel, payload in aprocess_notify(self.connection, self.channels, connected_event=self.events.ready_event):
            await dispatcher.process_message(payload, broker=self, channel=channel)

    async def notify(self, channel, payload=None) -> None:
        await apublish_message(self.connection, channel, payload=payload)

    async def shutdown(self) -> None:
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
        if not self.old_connection:
            if self.connection:
                await self.connection.close()
                self.connection = None
