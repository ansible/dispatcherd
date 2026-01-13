import asyncio
import json
import logging
import multiprocessing
from multiprocessing.context import BaseContext
from types import ModuleType
from typing import Any, Callable, Iterable

from ..config import LazySettings
from ..config import settings as global_settings
from ..worker.target import work_loop

logger = logging.getLogger(__name__)


class ProcessProxy:
    def __init__(
        self,
        args: Iterable | None = None,
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
        self._settings = settings
        self.ctx = multiprocessing.get_context(self.mp_context)
        self.finished_queue: multiprocessing.Queue = self.ctx.Queue()
        self._shutdown = False
        self._recreate_kwargs: dict[str, Any] = {"settings": settings}

        # Settings will be passed to the workers to initialize dispatcher settings
        settings_config: dict = settings.serialize()
        # Settings are passed as a JSON format string
        # JSON is more type-restrictive than python pickle, which multiprocessing otherwise uses
        # this assures we do not pass python objects inside of settings by accident
        self.settings_stash: str = json.dumps(settings_config)

    def send_finished_queue_stop(self, timeout: float | None = None) -> None:
        """Send the sentinel into the finished queue once."""
        try:
            if timeout is None:
                self.finished_queue.put('stop')
            else:
                self.finished_queue.put('stop', timeout=timeout)
        except Exception:
            logger.exception('Failed to send stop sentinel to finished queue')

    def create_process(  # type: ignore[no-untyped-def]
        self, args: Iterable[int | str | dict] | None = None, kwargs: dict | None = None, **proxy_kwargs
    ) -> ProcessProxy:
        "Returns a ProcessProxy object, which itself contains a Process object, but actual subprocess is not yet started"
        # kwargs allow passing target for substituting the work_loop for testing
        if kwargs is None:
            kwargs = {}
        kwargs['settings'] = self.settings_stash
        if self._shutdown:
            raise RuntimeError("ProcessManager is shut down")
        kwargs['finished_queue'] = self.finished_queue
        return ProcessProxy(args=args, kwargs=kwargs, ctx=self.ctx, **proxy_kwargs)

    async def read_finished(self, timeout: float | None = None) -> dict[str, str | int]:
        if self._shutdown:
            raise RuntimeError("ProcessManager is shut down")
        if timeout is None:
            return await asyncio.to_thread(self.finished_queue.get)
        return await asyncio.to_thread(self.finished_queue.get, timeout=timeout)

    def shutdown(self) -> None:
        self._shutdown = True
        self.send_finished_queue_stop()
        logger.debug('Closing finished queue')
        self.finished_queue.close()

    def recreate(self) -> "ProcessManager":
        return type(self)(**self._recreate_kwargs)

    def has_shutdown(self) -> bool:
        return self._shutdown


class ForkServerManager(ProcessManager):
    mp_context = 'forkserver'

    def __init__(self, preload_modules: list[str] | None = None, settings: LazySettings = global_settings):
        super().__init__(settings=settings)
        self.ctx.set_forkserver_preload(preload_modules if preload_modules else [])
        self._recreate_kwargs = {"settings": settings, "preload_modules": preload_modules}


class SpawnServerManager(ProcessManager):
    mp_context = 'spawn'
