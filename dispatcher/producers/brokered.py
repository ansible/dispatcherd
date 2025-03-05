import asyncio
import logging
from typing import Iterable, Optional

from ..protocols import Broker, DispatcherMain
from .base import BaseProducer

logger = logging.getLogger(__name__)


class BrokeredProducer(BaseProducer):
    def __init__(self, broker: Broker, close_on_exit: bool = True) -> None:
        self.production_task: Optional[asyncio.Task] = None
        self.broker = broker
        self.close_on_exit = close_on_exit
        self.dispatcher: Optional[DispatcherMain] = None
        super().__init__()

    async def start_producing(self, dispatcher: DispatcherMain) -> None:
        self.production_task = asyncio.create_task(self.produce_forever(dispatcher), name=f'{self.broker}_production')

    def all_tasks(self) -> Iterable[asyncio.Task]:
        if self.production_task:
            return [self.production_task]
        return []

    async def connected_callback(self) -> None:
        if self.events:
            self.events.ready_event.set()
        if self.dispatcher:
            await self.dispatcher.connected_callback(self)

    async def produce_forever(self, dispatcher: DispatcherMain) -> None:
        self.dispatcher = dispatcher
        async for channel, payload in self.broker.aprocess_notify(connected_callback=self.connected_callback):
            self.produced_count += 1
            reply_to, reply_payload = await dispatcher.process_message(payload, producer=self, channel=channel)
            if reply_to and reply_payload:
                await self.notify(channel=reply_to, message=reply_payload)

    async def notify(self, channel: Optional[str] = None, message: str = '') -> None:
        await self.broker.apublish_message(channel=channel, message=message)

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
        if self.close_on_exit:
            logger.debug(f'Closing {self.broker} connection')
            await self.broker.aclose()
