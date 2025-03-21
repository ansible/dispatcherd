def test_sigusr1_in_worker_code():
    """
    Verify code implementation uses SIGUSR1 (not SIGTERM)
    """
    import inspect

    from dispatcherd.service.pool import PoolWorker
    from dispatcherd.worker.task import WorkerSignalHandler

    code_init = inspect.getsource(WorkerSignalHandler.__init__)
    code_cancel = inspect.getsource(PoolWorker.cancel)
    assert "SIGUSR1" in code_init
    assert "SIGUSR1" in code_cancel
