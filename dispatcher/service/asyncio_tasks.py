import asyncio
import logging

logger = logging.getLogger(__name__)


def done_callback(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        logger.info(f'Ack that task {task.get_name()} was canceled')


def ensure_fatal(task: asyncio.Task) -> asyncio.Task:
    task.add_done_callback(done_callback)

    # address race condition if attached to task right away
    if task.done():
        task.result()

    return task  # nicety so this can be used as a wrapper
