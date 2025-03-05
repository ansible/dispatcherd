import asyncio
import logging

logger = logging.getLogger(__name__)


def ensure_fatal(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        logger.info(f'Ack that task {task.get_name()} was canceled')
