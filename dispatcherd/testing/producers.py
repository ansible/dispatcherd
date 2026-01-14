import asyncio
import logging
from typing import Any

from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)
CLEANUP_TIMEOUT = 5.0


async def wait_for_producers_ready(dispatcher: DispatcherMain) -> None:
    "Returns when all the producers have hit their ready event"
    tmp_tasks: list[asyncio.Task[Any]] = []
    exit_wait_task: asyncio.Task[Any] | None = None
    try:
        if not dispatcher.shared.exit_event.is_set():
            exit_wait_task = asyncio.create_task(
                dispatcher.shared.exit_event.wait(),
                name='wait_for_producers_exit_event',
            )
            tmp_tasks.append(exit_wait_task)

        for producer in dispatcher.producers:
            if dispatcher.shared.exit_event.is_set():
                logger.debug('Exit event set before all producers became ready; stopping wait early')
                return

            existing_tasks = list(producer.all_tasks())
            wait_task = asyncio.create_task(producer.events.ready_event.wait(), name=f'tmp_{producer}_wait_task')
            tmp_tasks.append(wait_task)
            existing_tasks.append(wait_task)

            if exit_wait_task is not None:
                existing_tasks.append(exit_wait_task)

            done, _ = await asyncio.wait(existing_tasks, return_when=asyncio.FIRST_COMPLETED)

            if exit_wait_task and exit_wait_task in done:
                logger.debug('Exit event triggered while waiting for %s to become ready', producer)
                return

            if wait_task in done:
                await wait_task
            else:
                producer.events.ready_event.set()  # exits wait_task, producer had error
    finally:
        cleanup_tasks = tuple(tmp_tasks)
        if cleanup_tasks:
            for task in cleanup_tasks:
                if task.done():
                    try:
                        await task
                    except Exception:
                        logger.exception(f'Error awaiting tmp task {task}')
                    continue
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=CLEANUP_TIMEOUT)
                except asyncio.CancelledError:
                    if task.cancelled():
                        continue
                    raise
                except asyncio.TimeoutError:
                    logger.warning('Timed out waiting for cleanup task %s to cancel', task.get_name())
                    raise
                except Exception:
                    logger.exception(f'Error awaiting previously canceled tmp task {task}')
