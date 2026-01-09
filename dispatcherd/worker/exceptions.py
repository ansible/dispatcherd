from typing import Any


class DispatcherCancel(Exception):
    pass


class DispatcherExit(Exception):
    """Raised by a task to request that the worker process exit."""

    def __init__(self, result: Any | None = '<exit>') -> None:
        super().__init__(result)
        self.result = result
