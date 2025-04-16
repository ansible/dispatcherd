import asyncio
import logging
import time
from typing import Optional, Union

from ..protocols import DispatcherMain
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from ..service.next_wakeup_runner import HasWakeup, NextWakeupRunner
from .base import BaseProducer

logger = logging.getLogger(__name__)


class ScheduleEntry(HasWakeup):
    def __init__(self, period: float, start_time: float, body: dict) -> None:
        self.period = period
        self.last_ran = start_time
        self.body = body

    def mark_run(self) -> None:
        self.last_ran = time.monotonic()

    def next_wakeup(self) -> Optional[float]:
        return self.last_ran + self.period


class ScheduledProducer(BaseProducer):
    def __init__(self, task_schedule: dict[str, dict[str, Union[int, str]]], shared: SharedAsyncObjectsProtocol) -> None:
        self.task_schedule = task_schedule
        self.schedule_entries: set[ScheduleEntry] = set()
        self.dispatcher: Optional[DispatcherMain] = None
        self.schedule_runner = NextWakeupRunner(self.schedule_entries, self.trigger_schedule, shared=shared, name='ScheduledProducer')
        super().__init__()

    async def trigger_schedule(self, entry: ScheduleEntry) -> None:
        entry.mark_run()
        self.produced_count += 1
        message = entry.body.copy()
        message['uuid'] = f'sch-{self.produced_count}'
        if self.dispatcher:
            await self.dispatcher.process_message(message)

    async def start_producing(self, dispatcher: DispatcherMain) -> None:
        self.dispatcher = dispatcher
        current_time = time.monotonic()

        for task_name, options in self.task_schedule.items():
            submission_options = options.copy()
            submission_options['task'] = task_name
            per_seconds = float(submission_options.pop('schedule'))

            entry = ScheduleEntry(period=per_seconds, start_time=current_time, body=submission_options)
            self.schedule_entries.add(entry)

        await self.schedule_runner.kick()

        if self.events:
            self.events.ready_event.set()

    def all_tasks(self) -> list[asyncio.Task]:
        return self.schedule_runner.all_tasks()

    async def shutdown(self) -> None:
        logger.info('Stopping scheduled tasks')
        await self.schedule_runner.kick()  # To ack shutdown
        self.schedule_entries = set()  # Avoids duplication, in case .start_producing is called again
