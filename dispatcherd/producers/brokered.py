import asyncio
import logging
from typing import Iterable, Optional

from ..protocols import Broker, DispatcherMain
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from .base import BaseProducer

logger = logging.getLogger(__name__)


class BrokeredProducer(BaseProducer):
    can_recycle = True

    def __init__(self, broker: Broker, shared: SharedAsyncObjectsProtocol) -> None:
        self.production_task: Optional[asyncio.Task] = None
        self.broker = broker
        self.dispatcher: Optional[DispatcherMain] = None
        self.recycle_count = 0
        super().__init__()

    async def recycle(self) -> None:
        await self.events.recycle_event.wait()
        logger.info('recycle event received, restarting producer')
        self.events.recycle_event.clear()
        if self.production_task and self.production_task.done():
            self.production_task = None
        else:
            raise RuntimeError('Programming error - recycle should not be called with production running')
        await self.shutdown()
        await asyncio.sleep(1)
        assert self.dispatcher
        await self.start_producing(self.dispatcher)
        self.record_recycle()

    def __str__(self) -> str:
        broker_module = self.broker.__module__.rsplit('.', 1)[-1]
        return f'{broker_module}-producer'

    def get_status_data(self) -> dict:
        data = super().get_status_data()
        data['recycle_count'] = self.recycle_count
        if hasattr(self.broker, 'get_self_check_stats'):
            data['broker_self_check'] = self.broker.get_self_check_stats()
        return data

    async def start_producing(self, dispatcher: DispatcherMain) -> None:
        self.production_task = asyncio.create_task(self.produce_forever(dispatcher), name=f'{self.broker.__module__}_production')

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
            reply_to, reply_payload = await dispatcher.process_message(payload, producer=self, channel=str(channel))
            if reply_to and reply_payload:
                await self.notify(channel=reply_to, origin=channel, message=reply_payload)

    async def notify(self, channel: Optional[str] = None, origin: Optional[int | str] = None, message: str = '') -> None:
        await self.broker.apublish_message(channel=channel, origin=origin, message=message)

    async def shutdown(self) -> None:
        if self.production_task:
            production_task = self.production_task
            production_task.cancel()
            try:
                await production_task
            except asyncio.CancelledError:
                if not production_task.cancelled():
                    raise  # the shutdown itself was cancelled
            self.production_task = None

        await self.broker.aclose()

    def record_recycle(self) -> None:
        self.recycle_count += 1
