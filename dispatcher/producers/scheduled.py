import asyncio
import logging
from typing import Union

from .base import BaseProducer

logger = logging.getLogger(__name__)


class ScheduledProducer(BaseProducer):
    def __init__(self, task_schedule: dict[str, dict[str, Union[int, str]]]):
        self.task_schedule = task_schedule
        self.scheduled_tasks: list[asyncio.Task] = []
        super().__init__()

    async def start_producing(self, dispatcher) -> None:
        for task_name, options in self.task_schedule.items():
            submission_options = options.copy()
            per_seconds = submission_options.pop('schedule')
            schedule_task = asyncio.create_task(self.run_schedule_forever(task_name, per_seconds, dispatcher, submission_options))
            self.scheduled_tasks.append(schedule_task)
            schedule_task.add_done_callback(dispatcher.fatal_error_callback)
        if self.events:
            self.events.ready_event.set()

    def all_tasks(self) -> list[asyncio.Task]:
        return self.scheduled_tasks

    async def run_schedule_forever(self, task_name: str, per_seconds, dispatcher, submission_options) -> None:
        logger.info(f"Starting task runner for {task_name} with interval {per_seconds} seconds")
        while True:
            await asyncio.sleep(per_seconds)
            logger.debug(f"Produced scheduled task: {task_name}")
            self.produced_count += 1
            message = submission_options.copy()
            message['task'] = task_name
            message['uuid'] = f'sch-{self.produced_count}'
            await dispatcher.process_message(message)

    async def shutdown(self) -> None:
        logger.info('Stopping scheduled tasks')
        for task in self.scheduled_tasks:
            task.cancel()
        try:
            await asyncio.gather(*self.scheduled_tasks, return_exceptions=False)
        except asyncio.CancelledError:
            pass

        self.scheduled_tasks = []
