import asyncio
import contextlib
import logging
import traceback
from typing import Any, AsyncGenerator

from ..config import DispatcherSettings
from ..factories import from_settings
from ..service.main import DispatcherMain

logger = logging.getLogger(__name__)


async def _dump_dispatcher_diagnostics(dispatcher: DispatcherMain) -> str:
    """Dump comprehensive diagnostic information about the dispatcher state for debugging."""
    lines = []
    lines.append("=" * 80)
    lines.append("WORKERS_READY TIMEOUT - DIAGNOSTIC DUMP")
    lines.append("=" * 80)

    # Dump worker pool state
    lines.append("\nWorker Pool Status:")
    lines.append(f"  Min workers: {dispatcher.pool.min_workers}")
    lines.append(f"  Max workers: {dispatcher.pool.max_workers}")
    lines.append(f"  Next worker ID: {dispatcher.pool.next_worker_id}")
    lines.append(f"  Finished count: {dispatcher.pool.finished_count}")
    lines.append(f"  Canceled count: {dispatcher.pool.canceled_count}")
    lines.append(f"  Status counts: {dispatcher.pool.status_counts}")

    # Dump individual worker states
    lines.append(f"\nIndividual Worker States ({len(dispatcher.pool.workers.workers)} workers):")
    for worker_id, worker in dispatcher.pool.workers.workers.items():
        lines.append(f"\n  Worker {worker_id}:")
        lines.append(f"    Status: {worker.status}")
        lines.append(f"    PID: {worker.process.pid}")
        lines.append(f"    Is alive: {worker.process.is_alive()}")
        lines.append(f"    Exit code: {worker.process.exitcode() if hasattr(worker.process, 'exitcode') else 'N/A'}")
        lines.append(f"    Finished count: {worker.finished_count}")
        lines.append(f"    Current task: {worker.current_task}")
        lines.append(f"    Is active cancel: {worker.is_active_cancel}")
        lines.append(f"    Expected alive: {worker.expected_alive}")
        lines.append(f"    Is ready: {worker.is_ready}")
        lines.append(f"    Counts for capacity: {worker.counts_for_capacity}")
        lines.append(f"    Inactive: {worker.inactive}")

        # If worker is in error state, show error details if available
        if worker.status == 'error':
            if worker.error_type or worker.error_message:
                lines.append("    ERROR DETAILS:")
                if worker.error_type:
                    lines.append(f"      Type: {worker.error_type}")
                if worker.error_message:
                    lines.append(f"      Message: {worker.error_message}")
                if worker.error_traceback:
                    lines.append("      Traceback:")
                    for line in worker.error_traceback.split('\n'):
                        if line:  # Skip empty lines
                            lines.append(f"        {line}")
            else:
                lines.append("    NOTE: Worker in error state but no error details received")
                lines.append("    This can happen if:")
                lines.append("      1. Worker was killed by signal (SIGSEGV, SIGKILL, etc)")
                lines.append("      2. Error event sent but not yet processed (race condition)")
                lines.append("      3. Worker failed during shutdown/cleanup")
                exitcode = worker.process.exitcode()
                if exitcode is not None:
                    lines.append(f"    Exit code available: {exitcode}")
                    if exitcode < 0:
                        lines.append(f"      -> Process was killed by signal {-exitcode}")
                    elif exitcode > 0:
                        lines.append(f"      -> Process exited with error code {exitcode}")

    # Dump pool tasks state
    lines.append("\nPool Task States:")
    if dispatcher.pool.read_results_task:
        lines.append("  read_results_task:")
        lines.append(f"    Done: {dispatcher.pool.read_results_task.done()}")
        lines.append(f"    Cancelled: {dispatcher.pool.read_results_task.cancelled()}")
        if dispatcher.pool.read_results_task.done():
            try:
                exc = dispatcher.pool.read_results_task.exception()
                if exc:
                    lines.append(f"    Exception: {exc}")
                    lines.append(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                lines.append(f"    Error getting exception: {e}")
    else:
        lines.append("  read_results_task: None")

    if dispatcher.pool.management_task:
        lines.append("  management_task:")
        lines.append(f"    Done: {dispatcher.pool.management_task.done()}")
        lines.append(f"    Cancelled: {dispatcher.pool.management_task.cancelled()}")
        if dispatcher.pool.management_task.done():
            try:
                exc = dispatcher.pool.management_task.exception()
                if exc:
                    lines.append(f"    Exception: {exc}")
                    lines.append(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                lines.append(f"    Error getting exception: {e}")
    else:
        lines.append("  management_task: None")

    # Dump timeout runner task state
    if hasattr(dispatcher.pool.timeout_runner, 'task') and dispatcher.pool.timeout_runner.task:
        lines.append("  timeout_runner.task:")
        lines.append(f"    Done: {dispatcher.pool.timeout_runner.task.done()}")
        lines.append(f"    Cancelled: {dispatcher.pool.timeout_runner.task.cancelled()}")
        if dispatcher.pool.timeout_runner.task.done():
            try:
                exc = dispatcher.pool.timeout_runner.task.exception()
                if exc:
                    lines.append(f"    Exception: {exc}")
                    lines.append(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                lines.append(f"    Error getting exception: {e}")

    # Dump all asyncio tasks
    lines.append("\nAll asyncio tasks:")
    all_tasks = asyncio.all_tasks()
    lines.append(f"  Total tasks: {len(all_tasks)}")
    for i, task in enumerate(all_tasks):
        lines.append(f"\n  Task {i}: {task.get_name()}")
        lines.append(f"    Coro: {task.get_coro()}")
        lines.append(f"    Done: {task.done()}")
        lines.append(f"    Cancelled: {task.cancelled()}")
        if task.done() and not task.cancelled():
            try:
                exc = task.exception()
                if exc:
                    lines.append(f"    Exception: {exc}")
                    lines.append(f"    Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}")
            except Exception as e:
                lines.append(f"    Error getting exception: {e}")

    # Dump event states
    lines.append("\nEvent States:")
    lines.append(f"  workers_ready: {dispatcher.pool.events.workers_ready.is_set()}")
    lines.append(f"  queue_cleared: {dispatcher.pool.events.queue_cleared.is_set()}")
    lines.append(f"  work_cleared: {dispatcher.pool.events.work_cleared.is_set()}")
    lines.append(f"  management_event: {dispatcher.pool.events.management_event.is_set()}")
    lines.append(f"  exit_event: {dispatcher.shared.exit_event.is_set()}")

    # Dump queue/blocker state
    lines.append("\nQueue/Blocker State:")
    lines.append(f"  Queued messages: {dispatcher.pool.queuer.count()}")
    lines.append(f"  Blocked messages: {dispatcher.pool.blocker.count()}")
    lines.append(f"  Active task count: {dispatcher.pool.active_task_ct()}")
    lines.append(f"  Running count: {dispatcher.pool.get_running_count()}")

    lines.append("=" * 80)

    return "\n".join(lines)


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
            diagnostics = await _dump_dispatcher_diagnostics(dispatcher)
            raise asyncio.TimeoutError(f"Workers ready timeout\n{diagnostics}")

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
