import asyncio
import logging
from typing import Union

from ..protocols import DispatcherMain
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from .base import BaseProducer

logger = logging.getLogger(__name__)


class OnStartProducer(BaseProducer):
    def __init__(self, task_list: dict[str, dict[str, Union[int, str]]], shared: SharedAsyncObjectsProtocol):
        self.task_list = task_list
        super().__init__()

    async def start_producing(self, dispatcher: DispatcherMain) -> None:
        self.events.ready_event.set()

        for task_name, options in self.task_list.items():
            message = options.copy()
            message['task'] = task_name
            message['uuid'] = f'on-start-{self.produced_count}'

            logger.debug(f"Produced on-start task: {task_name}")
            self.produced_count += 1
            await dispatcher.process_message(message)

    def all_tasks(self) -> list[asyncio.Task]:
        return []

    async def shutdown(self) -> None:
        pass
