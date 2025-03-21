import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class CallbackHolder:
    def __init__(self, exit_event: Optional[asyncio.Event]):
        self.exit_event = exit_event

    def done_callback(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            logger.info(f'Ack that task {task.get_name()} was canceled')
        except Exception:
            if self.exit_event:
                self.exit_event.set()
            raise


def ensure_fatal(task: asyncio.Task, exit_event: Optional[asyncio.Event] = None) -> asyncio.Task:
    holder = CallbackHolder(exit_event)
    task.add_done_callback(holder.done_callback)

    # address race condition if attached to task right away
    if task.done():
        try:
            task.result()
        except Exception:
            if exit_event:
                exit_event.set()
            raise

    return task  # nicety so this can be used as a wrapper
