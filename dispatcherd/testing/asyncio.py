import asyncio
import contextlib
import logging
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.asyncio_tasks import cancel_other_tasks
from ..service.main import DispatcherMain
from .producers import wait_for_producers_ready

logger = logging.getLogger(__name__)


def _get_pending_tasks() -> list[asyncio.Task[Any]]:
    current = asyncio.current_task()
    return [task for task in asyncio.all_tasks() if task is not current and not task.done()]


def _is_dispatcher_main_task(task: asyncio.Task[Any]) -> bool:
    coro = task.get_coro()
    code = getattr(coro, 'cr_code', None)
    qualname = getattr(code, 'co_qualname', None)
    return qualname == f'{DispatcherMain.__name__}.main_as_task'


@contextlib.asynccontextmanager
async def adispatcher_service(config: dict) -> AsyncGenerator[DispatcherMain, Any]:
    dispatcher = None
    try:
        settings = DispatcherSettings(config)
        dispatcher = from_settings(settings=settings)  # type: ignore[arg-type]

        # Note: dispatcher.connect_signals is expected to increase flakiness
        await asyncio.wait_for(dispatcher.start_working(), timeout=1)
        await asyncio.wait_for(wait_for_producers_ready(dispatcher), timeout=1)
        await asyncio.wait_for(dispatcher.pool.events.workers_ready.wait(), timeout=1)

        assert dispatcher.pool.finished_count == 0  # sanity
        assert dispatcher.control_count == 0

        yield dispatcher
    finally:
        if dispatcher:
            try:
                await asyncio.wait_for(dispatcher.shutdown(), timeout=5)
            except asyncio.TimeoutError:
                logger.error('Dispatcher shutdown timed out; inspecting tasks before cancellation')
            except Exception:
                logger.exception('shutdown had error')

            # If main task is still running, allow that to finish
            pending = _get_pending_tasks()
            if pending:
                main_tasks = [task for task in pending if _is_dispatcher_main_task(task)]
                if main_tasks:
                    for task in main_tasks:
                        try:
                            await asyncio.wait_for(asyncio.shield(task), timeout=2)
                        except asyncio.TimeoutError:
                            logger.warning('Dispatcher main_as_task task failed to finish within %s seconds: %r', 2, task)
                    pending = _get_pending_tasks()  # refresh pending list

            # Any tasks that are pending (main task resolved) should give an error
            if pending:
                for task in pending:
                    try:
                        is_cancelling = getattr(task, 'cancelling', lambda: None)()
                        s = f"{task!r} name={task.get_name()!r} done={task.done()} cancelled={task.cancelled()} cancelling={is_cancelling}"
                    except Exception as e:
                        s = f"<failed to describe task: {type(e).__name__}: {e}> task_type={type(task)!r} task_repr={task}"
                    logger.error("Task still pending after shutdown: %s", s)

                    task.cancel()

                # Give it some seconds for tasks to finish after cancel happening, but do not raise additional errors
                try:
                    await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=2.0)
                except asyncio.TimeoutError:
                    logger.error('Timed out waiting for pending tasks to cancel')

                raise RuntimeError('Pending asyncio tasks detected during dispatcher teardown')

            try:
                await cancel_other_tasks()
            except Exception:
                logger.exception('cancel_tasks had error')
