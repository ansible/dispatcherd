import inspect
import json
import logging
import time
from uuid import uuid4

from dispatcher.registry import DispatcherMethodRegistry, registry
from dispatcher.utils import DuplicateBehavior

logger = logging.getLogger('awx.main.dispatch')


class task:
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

    def __init__(self, queue=None, on_duplicate: str = DuplicateBehavior.parallel.value, registry: DispatcherMethodRegistry = registry):
        self.queue = queue
        self.on_duplicate = on_duplicate
        self.registry = registry

    def __call__(self, fn):
        dmethod = self.registry.register(fn)

        queue = self.queue
        on_duplicate = self.on_duplicate

        class PublisherMixin(object):
            queue = None

            @classmethod
            def delay(cls, *args, **kwargs):
                return cls.apply_async(args, kwargs)

            @classmethod
            def get_async_body(cls, args=None, kwargs=None, uuid=None, delay=None, **kw):
                """
                Get the python dict to become JSON data in the pg_notify message
                This same message gets passed over the dispatcher IPC queue to workers
                If a task is submitted to a multiprocessing pool, skipping pg_notify, this might be used directly
                """
                task_id = uuid or str(uuid4())
                args = args or []
                kwargs = kwargs or {}
                obj = {'uuid': task_id, 'args': args, 'kwargs': kwargs, 'task': cls.name, 'time_pub': time.time()}

                if delay is not None:
                    obj['delay'] = delay

                # TODO: callback to add other things, guid in case of AWX

                if on_duplicate != DuplicateBehavior.parallel.value:
                    obj['on_duplicate'] = on_duplicate

                obj.update(**kw)
                return obj

            @classmethod
            def apply_async(cls, args=None, kwargs=None, queue=None, uuid=None, delay=None, connection=None, config=None, on_duplicate=None, **kw):
                queue = queue or getattr(cls.queue, 'im_func', cls.queue)
                if not queue:
                    msg = f'{cls.name}: Queue value required and may not be None'
                    logger.error(msg)
                    raise ValueError(msg)

                if callable(queue):
                    queue = queue()

                obj = cls.get_async_body(args=args, kwargs=kwargs, uuid=uuid, delay=delay, **kw)
                if on_duplicate:
                    obj['on_duplicate'] = on_duplicate

                # TODO: before sending, consult an app-specific callback if configured
                from dispatcher.brokers.pg_notify import publish_message

                # NOTE: the kw will communicate things in the database connection data
                publish_message(queue, json.dumps(obj), connection=connection, config=config, **kw)
                return (obj, queue)

        # If the object we're wrapping *is* a class (e.g., RunJob), return
        # a *new* class that inherits from the wrapped class *and* BaseTask
        # In this way, the new class returned by our decorator is the class
        # being decorated *plus* PublisherMixin so cls.apply_async() and
        # cls.delay() work
        bases = []
        ns = {'name': dmethod.serialize_task(), 'queue': queue}
        if inspect.isclass(fn):
            bases = list(fn.__bases__)
            ns.update(fn.__dict__)
        cls = type(fn.__name__, tuple(bases + [PublisherMixin]), ns)
        if inspect.isclass(fn):
            return cls

        # if the object being decorated is *not* a class (it's a Python
        # function), make fn.apply_async and fn.delay proxy through to the
        # PublisherMixin we dynamically created above
        setattr(fn, 'name', cls.name)
        setattr(fn, 'apply_async', cls.apply_async)
        setattr(fn, 'delay', cls.delay)
        setattr(fn, 'get_async_body', cls.get_async_body)
        return fn
