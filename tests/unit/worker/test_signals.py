def test_sigusr1_in_worker_code():
    """
    Verify code implementation uses SIGUSR1 (not SIGTERM)
    """
    from dispatcher.worker.task import WorkerSignalHandler
    from dispatcher.service.pool import PoolWorker
    import inspect

    code_init = inspect.getsource(WorkerSignalHandler.__init__)
    code_cancel = inspect.getsource(PoolWorker.cancel)
    assert "SIGUSR1" in code_init
    assert "SIGUSR1" in code_cancel
