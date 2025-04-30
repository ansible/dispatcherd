import logging
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

from .config import LazySettings
from .config import settings as global_settings
from .protocols import ProcessorParams
from .registry import DispatcherMethodRegistry
from .registry import registry as default_registry
from .utils import DispatcherCallable

logger = logging.getLogger('awx.main.dispatch')


@dataclass(kw_only=True)
class CompatParams(ProcessorParams):
    on_duplicate: str

    def to_dict(self) -> dict[str, Any]:
        return {'on_duplicate': self.on_duplicate}

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> 'CompatParams':
        "Unused, only exists for adherence to protocol"
        return cls(on_duplicate=message['on_duplicate'])


class DispatcherDecorator:
    def __init__(
        self,
        registry: DispatcherMethodRegistry,
        *,
        bind: bool = False,
        decorate: bool = True,
        queue: Optional[str] = None,
        timeout: Optional[float] = None,
        processor_options: Iterable[ProcessorParams] = (),
        on_duplicate: Optional[str] = None,  # Deprecated
    ) -> None:
        self.registry = registry
        self.bind = bind
        self.decorate = decorate
        self.queue = queue
        self.timeout = timeout
        self.processor_options = processor_options
        self.on_duplicate = on_duplicate

    def __call__(self, fn: DispatcherCallable, /) -> DispatcherCallable:
        "Concrete task decorator, registers method and glues on some methods from the registry"

        processor_options: Iterable[ProcessorParams]
        if self.on_duplicate:
            processor_options = (CompatParams(on_duplicate=self.on_duplicate),)
        else:
            processor_options = self.processor_options

        dmethod = self.registry.register(fn, bind=self.bind, queue=self.queue, timeout=self.timeout, processor_options=processor_options)

        if self.decorate:
            setattr(fn, 'apply_async', dmethod.apply_async)
            setattr(fn, 'delay', dmethod.delay)

        return fn


def task(
    *,
    bind: bool = False,
    queue: Optional[str] = None,
    timeout: Optional[float] = None,
    on_duplicate: Optional[str] = None,  # Deprecated
    processor_options: Iterable[ProcessorParams] = (),
    registry: DispatcherMethodRegistry = default_registry,
) -> DispatcherDecorator:
    """
    Used to decorate a function or class so that it can be run asynchronously
    via the task dispatcherd.  Tasks can be simple functions:

    @task()
    def add(a, b):
        return a + b

    ...or classes that define a `run` method:

    @task()
    class Adder:
        def run(self, a, b):
            return a + b

    # Tasks can be run synchronously...
    assert add(1, 1) == 2
    assert Adder().run(1, 1) == 2

    # ...or published to a queue:
    add.apply_async([1, 1])
    Adder.apply_async([1, 1])

    # Tasks can also define a specific target queue or use the special fan-out queue tower_broadcast:

    @task(queue='slow-tasks')
    def snooze():
        time.sleep(10)

    @task(queue='tower_broadcast')
    def announce():
        print("Run this everywhere!")

    # The registry kwarg changes where the registration is saved, mainly for testing
    # The on_duplicate kwarg controls behavior when multiple instances of the task running
    # options are documented in dispatcherd.utils.DuplicateBehavior
    """
    return DispatcherDecorator(registry, bind=bind, queue=queue, timeout=timeout, processor_options=processor_options, on_duplicate=on_duplicate)


def submit_task(
    fn: DispatcherCallable,
    /,
    *,
    registry: DispatcherMethodRegistry = default_registry,
    queue: Optional[str] = None,
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    uuid: Optional[str] = None,
    bind: bool = False,
    timeout: Optional[float] = 0.0,
    processor_options: Iterable[ProcessorParams] = (),
    settings: LazySettings = global_settings,
) -> Tuple[dict, str]:
    dmethod = registry.get_from_callable(fn)

    return dmethod.apply_async(
        args=args, kwargs=kwargs, queue=queue, uuid=uuid, bind=bind, settings=settings, timeout=timeout, processor_options=processor_options
    )
