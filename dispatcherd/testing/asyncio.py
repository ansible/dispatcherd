import asyncio
import contextlib
import logging
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def adispatcher_service(config: dict) -> AsyncGenerator[DispatcherMain, Any]:
    dispatcher = None
    try:
        settings = DispatcherSettings(config)
        dispatcher = from_settings(settings=settings)  # type: ignore[arg-type]

        await asyncio.wait_for(dispatcher.connect_signals(), timeout=1)
        await asyncio.wait_for(dispatcher.start_working(), timeout=1)
        await asyncio.wait_for(dispatcher.wait_for_producers_ready(), timeout=1)
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
            pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
            if pending:
                for task in pending:
                    try:
                        s = f"{task!r} name={task.get_name()!r} done={task.done()} cancelled={task.cancelled()} cancelling={getattr(task,'cancelling',lambda:None)()}"
                    except Exception as e:
                        s = f"<failed to describe task: {type(e).__name__}: {e}> task_type={type(task)!r} task_repr={task}"
                    logger.error("Task still pending after shutdown: %s", s)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise RuntimeError('Pending asyncio tasks detected during dispatcher teardown')
            try:
                await dispatcher.cancel_tasks()
            except Exception:
                logger.exception('cancel_tasks had error')
