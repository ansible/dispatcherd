import logging
from typing import Optional

from dispatcher.registry import DispatcherMethodRegistry
from dispatcher.registry import registry as default_registry
from dispatcher.utils import DispatcherCallable

logger = logging.getLogger('awx.main.dispatch')


def _task_decorator(fn: DispatcherCallable, registry=default_registry, **kwargs) -> DispatcherCallable:
    "Core logic of the task decorator, register method and then glue on some methods from the registry"

    dmethod = registry.register(fn, **kwargs)

    setattr(fn, 'apply_async', dmethod.apply_async)
    setattr(fn, 'delay', dmethod.delay)

    return fn


def task(
    fn_maybe: Optional[DispatcherCallable] = None,
    queue: Optional[str] = None,
    on_duplicate: Optional[str] = None,
    registry: DispatcherMethodRegistry = default_registry,
):
    """
    Used to decorate a function or class so that it can be run asynchronously
    via the task dispatcher.  Tasks can be simple functions:

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
    """
    # Used directly as a decorator
    if fn_maybe and callable(fn_maybe):
        return _task_decorator(fn_maybe)

    # Called with argument, return a decorator
    def _local_task_decorator(fn: DispatcherCallable) -> DispatcherCallable:
        return _task_decorator(fn, queue=queue, on_duplicate=on_duplicate, registry=registry)

    return _local_task_decorator
