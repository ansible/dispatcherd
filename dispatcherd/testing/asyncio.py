import asyncio
import contextlib
import io
import logging
import traceback
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)


def _format_task_details(task: asyncio.Task) -> str:
    """Render pending/done task diagnostics so teardown can log a useful traceback."""
    details: list[str] = [f'Pending task during teardown: {task}']
    if task.done():
        try:
            exc = task.exception()
        except asyncio.InvalidStateError:
            exc = None
        if exc:
            tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            details.append('Task exception:\n' + tb)
        else:
            details.append('Task is marked done but did not provide an exception.')
        return '\n'.join(details)

    buffer = io.StringIO()
    try:
        task.print_stack(file=buffer)
        stack = buffer.getvalue() or '  <no stack available>'
    except Exception as stack_exc:  # pragma: no cover - diagnostic path
        stack = f'  <failed to capture stack: {stack_exc}>'
    details.append('Task stack:\n' + stack)
    return '\n'.join(details)


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
                    print('pending task details:')
                    print(_format_task_details(task))
                    logger.error(_format_task_details(task))
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                raise RuntimeError('Pending asyncio tasks detected during dispatcher teardown')
            try:
                await dispatcher.cancel_tasks()
            except Exception:
                logger.exception('cancel_tasks had error')
