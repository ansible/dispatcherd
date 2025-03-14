import asyncio
import multiprocessing
from multiprocessing.context import BaseContext
from types import ModuleType
from typing import Any, Callable, Iterable, Optional, Union

from ..config import LazySettings
from ..config import settings as global_settings
from ..worker.task import work_loop


class ProcessProxy:
    def __init__(
        self,
        args: Optional[Iterable] = None,
        kwargs: Optional[dict] = None,
        target: Callable = work_loop,
        ctx: Union[BaseContext, ModuleType] = multiprocessing,
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

    def join(self, timeout: Optional[int] = None) -> None:
        if timeout:
            self._process.join(timeout=timeout)
        else:
            self._process.join()

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid

    def exitcode(self) -> Optional[int]:
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

    def __exit__(self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[Any]) -> Optional[bool]:
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
        self.ctx = multiprocessing.get_context(self.mp_context)
        self.finished_queue: multiprocessing.Queue = self.ctx.Queue()
        self.settings_stash: dict = settings.serialize()  # These are passed to the workers to initialize dispatcher settings
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop:
            return self._loop
        self._loop = asyncio.get_event_loop()
        return self._loop

    def create_process(  # type: ignore[no-untyped-def]
        self, args: Optional[Iterable[int | str | dict]] = None, kwargs: Optional[dict] = None, **proxy_kwargs
    ) -> ProcessProxy:
        "Returns a ProcessProxy object, which itself contains a Process object, but actual subprocess is not yet started"
        # kwargs allow passing target for substituting the work_loop for testing
        if kwargs is None:
            kwargs = {}
        kwargs['settings'] = self.settings_stash
        kwargs['finished_queue'] = self.finished_queue
        return ProcessProxy(args=args, kwargs=kwargs, ctx=self.ctx, **proxy_kwargs)

    async def read_finished(self) -> dict[str, Union[str, int]]:
        message = await self.get_event_loop().run_in_executor(None, self.finished_queue.get)
        return message


class ForkServerManager(ProcessManager):
    mp_context = 'forkserver'

    def __init__(self, preload_modules: Optional[list[str]] = None, settings: LazySettings = global_settings):
        super().__init__(settings=settings)
        self.ctx.set_forkserver_preload(preload_modules if preload_modules else [])
