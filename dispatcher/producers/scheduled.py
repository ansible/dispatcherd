import asyncio
import logging

logger = logging.getLogger(__name__)


class ScheduledProducer:
    def __init__(self, task_schedule):
        self.task_schedule = task_schedule
        self.scheduled_tasks = []

    async def start_producing(self, dispatcher):
        for task_name, options in self.task_schedule.items():
            per_seconds = options['schedule'].total_seconds()
            self.scheduled_tasks.append(asyncio.create_task(self.run_schedule_forever(task_name, per_seconds, dispatcher)))

    def all_tasks(self):
        return self.scheduled_tasks

    async def run_schedule_forever(self, task_name, per_seconds, dispatcher):
        logger.info(f"Starting task runner for {task_name} with interval {per_seconds} seconds")
        while True:
            await asyncio.sleep(per_seconds)
            logger.debug(f"Produced scheduled task: {task_name}")
            await dispatcher.process_message(task_name)

    async def shutdown(self):
        logger.info('Stopping scheduled tasks')
        for task in self.scheduled_tasks:
            task.cancel()
        await asyncio.gather(*self.scheduled_tasks, return_exceptions=True)
        self.scheduled_tasks = []
