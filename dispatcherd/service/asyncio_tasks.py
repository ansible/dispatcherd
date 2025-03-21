import asyncio
import logging
from typing import Iterable, Optional

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


async def wait_for_any(events: Iterable[asyncio.Event]) -> int:
    """
    Wait for a list of events. If any of the events gets set, this function
    will return
    """
    tasks = [asyncio.create_task(event.wait()) for event in events]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)

    for i, task in enumerate(tasks):
        if task in done:
            return i

    raise RuntimeError('Internal error - could done find any tasks that are done')
