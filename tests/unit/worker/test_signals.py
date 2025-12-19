import signal


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
