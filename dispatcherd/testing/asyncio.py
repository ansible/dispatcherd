import asyncio
import contextlib
import logging
import traceback
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)


async def _dump_dispatcher_diagnostics(dispatcher: DispatcherMain) -> None:
    """Dump comprehensive diagnostic information about the dispatcher state for debugging."""
    logger.error("=" * 80)
    logger.error("WORKERS_READY TIMEOUT - DIAGNOSTIC DUMP")
    logger.error("=" * 80)

    # Dump worker pool state
    logger.error(f"\nWorker Pool Status:")
    logger.error(f"  Min workers: {dispatcher.pool.min_workers}")
    logger.error(f"  Max workers: {dispatcher.pool.max_workers}")
    logger.error(f"  Next worker ID: {dispatcher.pool.next_worker_id}")
    logger.error(f"  Finished count: {dispatcher.pool.finished_count}")
    logger.error(f"  Canceled count: {dispatcher.pool.canceled_count}")
    logger.error(f"  Status counts: {dispatcher.pool.status_counts}")

    # Dump individual worker states
    logger.error(f"\nIndividual Worker States ({len(dispatcher.pool.workers.workers)} workers):")
    for worker_id, worker in dispatcher.pool.workers.workers.items():
        logger.error(f"\n  Worker {worker_id}:")
        logger.error(f"    Status: {worker.status}")
        logger.error(f"    PID: {worker.process.pid}")
        logger.error(f"    Is alive: {worker.process.is_alive()}")
        logger.error(f"    Exit code: {worker.process.exitcode() if hasattr(worker.process, 'exitcode') else 'N/A'}")
        logger.error(f"    Finished count: {worker.finished_count}")
        logger.error(f"    Current task: {worker.current_task}")
        logger.error(f"    Is active cancel: {worker.is_active_cancel}")
        logger.error(f"    Expected alive: {worker.expected_alive}")
        logger.error(f"    Is ready: {worker.is_ready}")
        logger.error(f"    Counts for capacity: {worker.counts_for_capacity}")
        logger.error(f"    Inactive: {worker.inactive}")

    # Dump pool tasks state
    logger.error(f"\nPool Task States:")
    if dispatcher.pool.read_results_task:
        logger.error(f"  read_results_task:")
        logger.error(f"    Done: {dispatcher.pool.read_results_task.done()}")
        logger.error(f"    Cancelled: {dispatcher.pool.read_results_task.cancelled()}")
        if dispatcher.pool.read_results_task.done():
            try:
                exc = dispatcher.pool.read_results_task.exception()
                if exc:
                    logger.error(f"    Exception: {exc}")
                    logger.error(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                logger.error(f"    Error getting exception: {e}")
    else:
        logger.error(f"  read_results_task: None")

    if dispatcher.pool.management_task:
        logger.error(f"  management_task:")
        logger.error(f"    Done: {dispatcher.pool.management_task.done()}")
        logger.error(f"    Cancelled: {dispatcher.pool.management_task.cancelled()}")
        if dispatcher.pool.management_task.done():
            try:
                exc = dispatcher.pool.management_task.exception()
                if exc:
                    logger.error(f"    Exception: {exc}")
                    logger.error(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                logger.error(f"    Error getting exception: {e}")
    else:
        logger.error(f"  management_task: None")

    # Dump timeout runner task state
    if hasattr(dispatcher.pool.timeout_runner, 'task') and dispatcher.pool.timeout_runner.task:
        logger.error(f"  timeout_runner.task:")
        logger.error(f"    Done: {dispatcher.pool.timeout_runner.task.done()}")
        logger.error(f"    Cancelled: {dispatcher.pool.timeout_runner.task.cancelled()}")
        if dispatcher.pool.timeout_runner.task.done():
            try:
                exc = dispatcher.pool.timeout_runner.task.exception()
                if exc:
                    logger.error(f"    Exception: {exc}")
                    logger.error(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                logger.error(f"    Error getting exception: {e}")

    # Dump all asyncio tasks
    logger.error(f"\nAll asyncio tasks:")
    all_tasks = asyncio.all_tasks()
    logger.error(f"  Total tasks: {len(all_tasks)}")
    for i, task in enumerate(all_tasks):
        logger.error(f"\n  Task {i}: {task.get_name()}")
        logger.error(f"    Coro: {task.get_coro()}")
        logger.error(f"    Done: {task.done()}")
        logger.error(f"    Cancelled: {task.cancelled()}")
        if task.done() and not task.cancelled():
            try:
                exc = task.exception()
                if exc:
                    logger.error(f"    Exception: {exc}")
                    logger.error(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                logger.error(f"    Error getting exception: {e}")

    # Dump event states
    logger.error(f"\nEvent States:")
    logger.error(f"  workers_ready: {dispatcher.pool.events.workers_ready.is_set()}")
    logger.error(f"  queue_cleared: {dispatcher.pool.events.queue_cleared.is_set()}")
    logger.error(f"  work_cleared: {dispatcher.pool.events.work_cleared.is_set()}")
    logger.error(f"  management_event: {dispatcher.pool.events.management_event.is_set()}")
    logger.error(f"  exit_event: {dispatcher.shared.exit_event.is_set()}")

    # Dump queue/blocker state
    logger.error(f"\nQueue/Blocker State:")
    logger.error(f"  Queued messages: {dispatcher.pool.queuer.count()}")
    logger.error(f"  Blocked messages: {dispatcher.pool.blocker.count()}")
    logger.error(f"  Active task count: {dispatcher.pool.active_task_ct()}")
    logger.error(f"  Running count: {dispatcher.pool.get_running_count()}")

    logger.error("=" * 80)


@contextlib.asynccontextmanager
async def adispatcher_service(config: dict) -> AsyncGenerator[DispatcherMain, Any]:
    dispatcher = None
    try:
        settings = DispatcherSettings(config)
        dispatcher = from_settings(settings=settings)  # type: ignore[arg-type]

        await asyncio.wait_for(dispatcher.connect_signals(), timeout=1)
        await asyncio.wait_for(dispatcher.start_working(), timeout=1)
        await asyncio.wait_for(dispatcher.wait_for_producers_ready(), timeout=1)

        try:
            await asyncio.wait_for(dispatcher.pool.events.workers_ready.wait(), timeout=3)
        except asyncio.TimeoutError:
            await _dump_dispatcher_diagnostics(dispatcher)
            raise

        assert dispatcher.pool.finished_count == 0  # sanity
        assert dispatcher.control_count == 0

        yield dispatcher
    finally:
        if dispatcher:
            try:
                await dispatcher.shutdown()
                await dispatcher.cancel_tasks()
            except Exception:
                logger.exception('shutdown had error')
