import logging
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

from .config import LazySettings
from .config import settings as global_settings
from .protocols import ProcessorParams
from .registry import DispatcherMethod, DispatcherMethodRegistry, NotRegistered
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
    decorate: bool = True,
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
    return DispatcherDecorator(
        registry, bind=bind, queue=queue, timeout=timeout, processor_options=processor_options, on_duplicate=on_duplicate, decorate=decorate
    )


def submit_task(
    fn: DispatcherCallable,
    /,
    *,
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    uuid: Optional[str] = None,
    queue: Optional[str] = None,
    timeout: Optional[float] = 0.0,
    processor_options: Iterable[ProcessorParams] = (),
    # Testing-oriented parameters
    bind: bool = False,
    registry: DispatcherMethodRegistry = default_registry,
    settings: LazySettings = global_settings,
) -> Tuple[dict, str]:
    """Submit a task for background execution via dispatcherd service(s)

    Example:
        from dispatcherd.processors.blocker import Blocker
        from tests.data.methods import hello_world

        submit_task(
            test_methods.sleep_function,
            processor_options=(Blocker.Params(on_duplicate='serial'),)
        )

    Parameters:
        fn: The function to run, must be registered with via @task() decorator
        args: Positional arguments to pass to the function when it runs
        kwargs: Keyword arguments to pass to the function when it runs
        uuid: Task UUID for tracking, if None, one will be automatically generated
        queue: The name of the target queue to submit the task to, if None, uses the default queue
        timeout: Optional timeout for background task, default is no timeout
        processor_options:

    Testing Focused Parameter, these would be uncommon to use, but are available for completeness and testing:
        bind: If True, the first argument passed to the function will be for dispatcherd interaction
                Normally it would make more sense to set via @task(bind=True) decorator
        registry: The task registry, normally you should use the default global registry
        settings: dispatcherd settings, normally you should use the default global settings
    """
    try:
        dmethod = registry.get_from_callable(fn)
    except NotRegistered:
        # For unregistered methods, create a local object
        dmethod = DispatcherMethod(fn)

    return dmethod.apply_async(
        args=args, kwargs=kwargs, queue=queue, uuid=uuid, bind=bind, settings=settings, timeout=timeout, processor_options=processor_options
    )
