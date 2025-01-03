import asyncio
import logging

from dispatcher.producers.base import BaseProducer

logger = logging.getLogger(__name__)


class ScheduledProducer(BaseProducer):
    def __init__(self, task_schedule: dict):
        self.events = self._create_events()
        self.task_schedule = task_schedule
        self.scheduled_tasks: list[asyncio.Task] = []
        self.produced_count = 0

    async def start_producing(self, dispatcher) -> None:
        for task_name, options in self.task_schedule.items():
            per_seconds = options['schedule']
            schedule_task = asyncio.create_task(self.run_schedule_forever(task_name, per_seconds, dispatcher))
            self.scheduled_tasks.append(schedule_task)
            schedule_task.add_done_callback(dispatcher.fatal_error_callback)
        self.events.ready_event.set()

    def all_tasks(self) -> list[asyncio.Task]:
        return self.scheduled_tasks

    async def run_schedule_forever(self, task_name: str, per_seconds, dispatcher) -> None:
        logger.info(f"Starting task runner for {task_name} with interval {per_seconds} seconds")
        while True:
            await asyncio.sleep(per_seconds)
            logger.debug(f"Produced scheduled task: {task_name}")
            self.produced_count += 1
            await dispatcher.process_message({'task': task_name, 'uuid': f'sch-{self.produced_count}'})

    async def shutdown(self) -> None:
        logger.info('Stopping scheduled tasks')
        for task in self.scheduled_tasks:
            task.cancel()
        try:
            await asyncio.gather(*self.scheduled_tasks, return_exceptions=False)
        except asyncio.CancelledError:
            pass
        except Exception:
            # traceback logged in fatal callback
            if not hasattr(task, '_dispatcher_tb_logged'):
                logger.exception('Pool shutdown saw an unexpected exception from results task')
        self.scheduled_tasks = []
