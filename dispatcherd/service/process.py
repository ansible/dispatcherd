import asyncio
import json
import logging
import multiprocessing
import os
import sys
from multiprocessing.context import BaseContext
from types import ModuleType
from typing import Any, Callable, Iterable, Optional

from ..config import LazySettings
from ..config import settings as global_settings
from ..worker.target import work_loop

logger = logging.getLogger(__name__)


def _is_gil_enabled() -> bool:
    """Return True when running under the traditional GIL build."""
    check = getattr(sys, "_is_gil_enabled", None)
    if callable(check):
        try:
            return bool(check())
        except Exception:
            logger.debug("sys._is_gil_enabled() check failed", exc_info=True)
    return True


def _fork_supported() -> bool:
    """Detect whether we can rely on fork-based multiprocessing semantics."""
    return hasattr(os, "fork") and _is_gil_enabled()


def _resolve_context(preferred: str) -> tuple[BaseContext, str]:
    """Return a multiprocessing context with fallback when necessary."""
    candidates: list[str] = []
    if preferred in ("fork", "forkserver") and not _fork_supported():
        logger.debug("Fork-based start method '%s' unsupported; will try spawn fallback", preferred)
    else:
        candidates.append(preferred)
    if "spawn" not in candidates:
        candidates.append("spawn")

    last_error: Optional[Exception] = None
    for name in candidates:
        try:
            ctx = multiprocessing.get_context(name)
            return ctx, name
        except (ValueError, RuntimeError) as exc:
            last_error = exc
            logger.debug("Failed to acquire multiprocessing context '%s': %s", name, exc)

    assert last_error is not None  # defensive
    raise RuntimeError(f"Could not obtain multiprocessing context (attempted: {candidates})") from last_error


class ProcessProxy:
    def __init__(
        self,
        args: Iterable[Any] | None = None,
        kwargs: dict | None = None,
        target: Callable = work_loop,
        ctx: BaseContext | ModuleType = multiprocessing,
    ) -> None:
        self.message_queue: multiprocessing.Queue = ctx.Queue()
        # This is intended use of multiprocessing context, but not available on BaseContext
        if kwargs is None:
            kwargs = {}
        kwargs['message_queue'] = self.message_queue
        if args is None:
            args = ()
        self._process = ctx.Process(target=target, args=args, kwargs=kwargs)  # type: ignore

    def start(self) -> None:
        self._process.start()

    def join(self, timeout: int | None = None) -> None:
        if timeout:
            self._process.join(timeout=timeout)
        else:
            self._process.join()

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def exitcode(self) -> int | None:
        return self._process.exitcode

    def is_alive(self) -> bool:
        return self._process.is_alive()

    def kill(self) -> None:
        self._process.kill()

    def terminate(self) -> None:
        self._process.terminate()

    def __enter__(self) -> "ProcessProxy":
        """Enter the runtime context and return this ProcessProxy."""
        return self

    def __exit__(self, exc_type: type | None, exc_value: BaseException | None, traceback: Any | None) -> bool | None:
        """Ensure the process is terminated and joined when exiting the context.

        If the process is still alive, it will be terminated (or killed if necessary) and then joined.
        """
        if self.is_alive():
            try:
                self.terminate()
            except Exception:
                self.kill()
        self.join()
        return None


class ProcessManager:
    mp_context = 'fork'

    def __init__(self, settings: LazySettings = global_settings) -> None:
        parent_proc = multiprocessing.parent_process()
        current_start_method = multiprocessing.get_start_method(allow_none=True)
        if parent_proc is not None and current_start_method in ('fork', 'forkserver'):
            raise RuntimeError(
                f"dispatcherd must run from the main process; current process was started via '{current_start_method}' under PID {parent_proc.pid}"
            )
        self.ctx, self.start_method = _resolve_context(self.mp_context)
        if self.start_method != self.mp_context:
            logger.info(
                "Multiprocessing start method '%s' unavailable; using '%s' instead",
                self.mp_context,
                self.start_method,
            )
        self._finished_queue: Optional[multiprocessing.Queue] = self.ctx.Queue()

        # Settings will be passed to the workers to initialize dispatcher settings
        settings_config: dict = settings.serialize()
        # Settings are passed as a JSON format string
        # JSON is more type-restrictive than python pickle, which multiprocessing otherwise uses
        # this assures we do not pass python objects inside of settings by accident
        self.settings_stash: str = json.dumps(settings_config)

        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def finished_queue(self) -> multiprocessing.Queue:
        """The processor manager owns the lifecycle of the finished queue, so if pool is restarted we have to handle this"""
        if self._finished_queue:
            return self._finished_queue
        self._finished_queue = self.ctx.Queue()
        return self._finished_queue

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop:
            return self._loop
        self._loop = asyncio.get_event_loop()
        return self._loop

    def create_process(  # type: ignore[no-untyped-def]
        self, args: Iterable[int | str | dict] | None = None, kwargs: dict | None = None, **proxy_kwargs
    ) -> ProcessProxy:
        "Returns a ProcessProxy object, which itself contains a Process object, but actual subprocess is not yet started"
        # kwargs allow passing target for substituting the work_loop for testing
        if kwargs is None:
            kwargs = {}
        kwargs['settings'] = self.settings_stash
        kwargs['finished_queue'] = self.finished_queue
        return ProcessProxy(args=args, kwargs=kwargs, ctx=self.ctx, **proxy_kwargs)

    async def read_finished(self) -> dict[str, str | int]:
        message = await self.get_event_loop().run_in_executor(None, self.finished_queue.get)
        return message

    def shutdown(self) -> None:
        if self._finished_queue:
            logger.debug('Closing finished queue')
            self._finished_queue.close()
            self._finished_queue = None


class ForkServerManager(ProcessManager):
    mp_context = 'forkserver'

    def __init__(self, preload_modules: list[str] | None = None, settings: LazySettings = global_settings):
        super().__init__(settings=settings)
        if self.start_method == 'forkserver' and hasattr(self.ctx, "set_forkserver_preload"):
            self.ctx.set_forkserver_preload(preload_modules if preload_modules else [])
        elif preload_modules:
            logger.debug("Forkserver preload requested but start method is '%s'; ignoring preload list", self.start_method)


class SpawnServerManager(ProcessManager):
    mp_context = 'spawn'
