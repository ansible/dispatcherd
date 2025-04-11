import asyncio
import json
import logging
from typing import Optional

from ..protocols import DispatcherMain
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from .base import BaseProducer

logger = logging.getLogger(__name__)


class ControlProducer(BaseProducer):
    """Placeholder producer to allow control actions to start tasks

    This must be enabled to start tasks via control actions.
    Indirectly, this also allows tasks to start other tasks.
    """

    def __init__(self, shared: SharedAsyncObjectsProtocol) -> None:
        self.dispatcher: Optional[DispatcherMain] = None
        super().__init__()

    async def start_producing(self, dispatcher: DispatcherMain) -> None:
        self.dispatcher = dispatcher
        self.events.ready_event.set()

    async def submit_task(self, data: dict) -> None:
        assert self.dispatcher is not None
        await self.dispatcher.process_message(json.dumps(data))
        self.produced_count += 1

    def all_tasks(self) -> list[asyncio.Task]:
        return []

    async def shutdown(self) -> None:
        pass
