import asyncio
import logging
from typing import Iterable, Optional

from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol

logger = logging.getLogger(__name__)


class SharedAsyncObjects(SharedAsyncObjectsProtocol):
    def __init__(self) -> None:
        # General exit event for program
        self.exit_event = asyncio.Event()
        # Lock for file descriptor mgmnt - hold lock when forking or connecting, to avoid DNS hangs
        # psycopg is well-behaved IFF you do not connect while forking, compare to AWX __clean_on_fork__
        self.forking_and_connecting_lock = asyncio.Lock()


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


async def wait_for_any(events: Iterable[asyncio.Event], names: Optional[Iterable[str]] = None) -> int:
    """
    Wait for a list of events. If any of the events gets set, this function
    will return
    """
    if names:
        tasks = [asyncio.create_task(event.wait(), name=task_name) for (event, task_name) in zip(events, names)]
    else:
        tasks = [asyncio.create_task(event.wait()) for event in events]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    for i, task in enumerate(tasks):
        if task in done:
            return i

    raise RuntimeError('Internal error - could done find any tasks that are done')


async def named_wait(event: asyncio.Event, name: str) -> None:
    """Add a name to waiting task so it is visible via debugging commands"""
    current_task = asyncio.current_task()
    if current_task:
        current_task.set_name(name)

    await event.wait()


async def cancel_other_tasks(*, timeout: float = 5.0) -> None:
    current = asyncio.current_task()
    if current is None:
        return

    tasks = {t for t in asyncio.all_tasks() if t is not current and not t.done()}
    if not tasks:
        return

    # Request cancellation for all tasks first.
    for t in tasks:
        if t.cancelling():
            logger.debug("Task %s already cancelling", t.get_name())
        else:
            logger.warning("Requesting cancel of lingering task %s", t.get_name())
            t.cancel()

    done, pending = await asyncio.wait(tasks, timeout=timeout)

    # Drain done tasks so exceptions are observed.
    for t in done:
        try:
            await asyncio.shield(t)
        except asyncio.CancelledError:
            # If the drained task ended up cancelled, that's expected.
            if t.cancelled():
                continue
            # Otherwise, *we* were cancelled.
            raise
        except Exception:
            logger.exception("Task %s raised while awaiting shutdown completion", t)

    # Report anything still pending after timeout.
    for t in pending:
        logger.warning(
            "Timed out waiting %.1fs for task %s to finish; leaving it cancelled",
            timeout,
            t.get_name(),
        )
