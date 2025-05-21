import logging
import signal
from queue import SimpleQueue

import pytest

import dispatcherd.worker.task as worker_task
from dispatcherd.publish import task
from dispatcherd.worker.exceptions import DispatcherCancel
from dispatcherd.worker.task import TaskWorker, WorkerSignalHandler


def test_sigusr1_in_worker_code():
    """
    Verify code implementation uses SIGUSR1 (not SIGTERM)
    """
    import inspect

    from dispatcherd.service.pool import PoolWorker
    from dispatcherd.worker.task import WorkerSignalHandler

    code_init = inspect.getsource(WorkerSignalHandler.enter_idle_mode)
    code_cancel = inspect.getsource(PoolWorker.cancel)
    assert "SIGUSR1" in code_init
    assert "SIGUSR1" in code_cancel


def test_worker_signal_handler_modes(monkeypatch):
    from dispatcherd.worker.task import WorkerSignalHandler

    registered: dict[int, object] = {}

    def fake_signal(sig: int, handler: object) -> object:
        registered[sig] = handler
        return handler

    monkeypatch.setattr(signal, "signal", fake_signal)

    handler = WorkerSignalHandler(worker_id=42)

    def assert_bound(value: object, func: object) -> None:
        assert hasattr(value, "__func__")
        assert value.__func__ is func  # type: ignore[attr-defined]

    assert_bound(registered[signal.SIGUSR1], WorkerSignalHandler.task_cancel)
    assert_bound(registered[signal.SIGINT], WorkerSignalHandler.exit_gracefully)
    assert_bound(registered[signal.SIGTERM], WorkerSignalHandler.exit_gracefully)

    handler.enter_task()
    assert registered[signal.SIGINT] is signal.SIG_DFL
    assert registered[signal.SIGTERM] is signal.SIG_DFL
    assert_bound(registered[signal.SIGUSR1], WorkerSignalHandler.task_cancel)

    handler.enter_idle_mode()
    assert_bound(registered[signal.SIGINT], WorkerSignalHandler.exit_gracefully)
    assert_bound(registered[signal.SIGTERM], WorkerSignalHandler.exit_gracefully)
    assert_bound(registered[signal.SIGUSR1], WorkerSignalHandler.task_cancel)


def test_worker_signal_handler_task_cancel_raises():
    handler = WorkerSignalHandler(worker_id=99)
    with pytest.raises(DispatcherCancel):
        handler.task_cancel()


def test_should_exit_after_parent_change(monkeypatch, registry, caplog):
    state = {'ppid': 100}

    def fake_getppid() -> int:
        return state['ppid']

    monkeypatch.setattr(worker_task.os, 'getppid', fake_getppid)

    queue = SimpleQueue()
    worker = TaskWorker(worker_id=7, registry=registry, message_queue=queue, finished_queue=queue)

    caplog.set_level(logging.ERROR)
    state['ppid'] = 200
    assert worker.should_exit() is True
    assert 'parent PID changed' in caplog.text


def test_should_exit_after_signal_request(registry, caplog):
    queue = SimpleQueue()
    worker = TaskWorker(worker_id=5, registry=registry, message_queue=queue, finished_queue=queue)

    caplog.set_level(logging.INFO)
    worker.signal_handler.exit_gracefully()
    assert worker.signal_handler.kill_now is True
    assert worker.should_exit() is True
    assert f'Worker {worker.worker_id} received worker process exit signal' in caplog.text


def test_run_callable_logs_cancel(registry, caplog):
    queue = SimpleQueue()

    @task(registry=registry)
    def cancel_task():
        raise DispatcherCancel()

    dmethod = registry.get_from_callable(cancel_task)

    worker = TaskWorker(worker_id=11, registry=registry, message_queue=queue, finished_queue=queue)

    caplog.set_level(logging.ERROR)
    result = worker.run_callable({"task": dmethod.serialize_task(), "uuid": "abc123"})

    assert result == '<cancel>'
    assert 'task canceled' in caplog.text
