import logging
from typing import Optional

from .registry import DispatcherMethodRegistry
from .registry import registry as default_registry
from .utils import DispatcherCallable

logger = logging.getLogger('awx.main.dispatch')


class DispatcherDecorator:
    def __init__(
        self,
        registry: DispatcherMethodRegistry,
        *,
        bind: bool = False,
        queue: Optional[str] = None,
        on_duplicate: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.registry = registry
        self.bind = bind
        self.queue = queue
        self.on_duplicate = on_duplicate
        self.timeout = timeout

    def __call__(self, fn: DispatcherCallable, /) -> DispatcherCallable:
        "Concrete task decorator, registers method and glues on some methods from the registry"

        dmethod = self.registry.register(fn, bind=self.bind, queue=self.queue, on_duplicate=self.on_duplicate, timeout=self.timeout)

        setattr(fn, 'apply_async', dmethod.apply_async)
        setattr(fn, 'delay', dmethod.delay)

        return fn


def task(
    *,
    bind: bool = False,
    queue: Optional[str] = None,
    on_duplicate: Optional[str] = None,
    timeout: Optional[float] = None,
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
    return DispatcherDecorator(registry, bind=bind, queue=queue, on_duplicate=on_duplicate, timeout=timeout)
