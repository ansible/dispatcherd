import asyncio
import os
import signal
import time
from typing import AsyncIterator

import pytest
import pytest_asyncio

from dispatcherd.protocols import DispatcherMain
from dispatcherd.testing.asyncio import adispatcher_service

TASK_PREFIX = "tests.data.signal_tasks"


@pytest.fixture(scope='session')
def worker_signal_config() -> dict:
    import multiprocessing

    ctx = multiprocessing.get_context()
    try:
        lock = ctx.Lock()
        lock.acquire()
        lock.release()
    except OSError as exc:
        pytest.skip(f"multiprocessing locks unavailable: {exc}")
    return {
        "version": 2,
        "service": {"main_kwargs": {"node_id": "worker-signal-tests"}},
        "worker": {"worker_kwargs": {"idle_timeout": 0.1}},
    }


@pytest_asyncio.fixture
async def aworker_dispatcher(worker_signal_config: dict) -> AsyncIterator[DispatcherMain]:
    async with adispatcher_service(worker_signal_config) as dispatcher:
        yield dispatcher


def _first_worker(dispatcher: DispatcherMain):
    return next(iter(dispatcher.pool.workers))


async def _run_task(dispatcher: DispatcherMain, task: str, uuid: str) -> None:
    dispatcher.pool.events.work_cleared.clear()
    await dispatcher.process_message({'task': f'{TASK_PREFIX}.{task}', 'uuid': uuid})
    await asyncio.wait_for(dispatcher.pool.events.work_cleared.wait(), timeout=5)


@pytest.mark.asyncio
async def test_idle_worker_sigterm(aworker_dispatcher: DispatcherMain):
    pool = aworker_dispatcher.pool
    worker = _first_worker(aworker_dispatcher)
    pid = worker.process.pid
    assert pid

    os.kill(pid, signal.SIGTERM)
    await asyncio.wait_for(worker.exit_msg_event.wait(), timeout=5)

    assert pool.finished_count == 0
    assert pool.canceled_count == 0
    assert worker.status == 'exited'


@pytest.mark.asyncio
async def test_dispatcher_exit_marks_worker_stopping(aworker_dispatcher: DispatcherMain):
    pool = aworker_dispatcher.pool
    worker = _first_worker(aworker_dispatcher)

    await _run_task(aworker_dispatcher, 'dispatcher_exit_task', uuid='dispatcher-exit')

    assert pool.finished_count == 1
    assert worker.stopping_at is not None
    await asyncio.wait_for(worker.exit_msg_event.wait(), timeout=5)
    assert worker.status == 'exited'


@pytest.mark.asyncio
async def test_sigint_with_task_handler_updates_counts(aworker_dispatcher: DispatcherMain):
    pool = aworker_dispatcher.pool
    worker = _first_worker(aworker_dispatcher)
    pid = worker.process.pid
    assert pid

    async def start_task():
        await _run_task(aworker_dispatcher, 'sigint_handler_task', uuid='sigint-exit')

    task = asyncio.create_task(start_task())
    await asyncio.sleep(0.2)
    os.kill(pid, signal.SIGINT)
    await task

    assert pool.finished_count >= 1
    assert worker.stopping_at is not None
    await asyncio.wait_for(worker.exit_msg_event.wait(), timeout=5)
    assert worker.status == 'exited'


@pytest.mark.asyncio
async def test_sigterm_interrupts_running_task(aworker_dispatcher: DispatcherMain):
    pool = aworker_dispatcher.pool
    worker = _first_worker(aworker_dispatcher)
    pid = worker.process.pid
    assert pid

    await aworker_dispatcher.process_message({'task': f'{TASK_PREFIX}.wait_forever', 'uuid': 'sigterm-long'})
    await asyncio.sleep(0.02)
    start = time.monotonic()
    os.kill(pid, signal.SIGTERM)
    await asyncio.wait_for(pool.stop_workers(), timeout=5)
    duration = time.monotonic() - start
    assert duration < 1.0
    assert worker.status == 'error'
    assert worker.exit_msg_event.is_set()
