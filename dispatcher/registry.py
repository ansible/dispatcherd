import inspect
import json
import logging
import threading
import time
from typing import Optional, Set
from uuid import uuid4

from dispatcher.utils import MODULE_METHOD_DEL, DispatcherCallable, DuplicateBehavior, resolve_callable

logger = logging.getLogger(__name__)


class DispatcherError(RuntimeError):
    pass


class NotRegistered(DispatcherError):
    pass


class InvalidMethod(DispatcherError):
    pass


class MethodRun:
    def __init__(self, message: dict):
        self.published: Optional[float] = None
        self.received: Optional[float] = None
        self.dispatched: Optional[float] = None
        self.finished: Optional[float] = None


class DispatcherMethod:
    def __init__(self, fn: DispatcherCallable, queue: Optional[str] = None, on_duplicate: Optional[str] = None):
        if not hasattr(fn, '__qualname__'):
            raise InvalidMethod('Can only register methods and classes')
        self.fn = fn
        self.queue = queue
        self.on_duplicate = on_duplicate
        self.call_ct: int = 0
        self.runtime: float = 0.0

    def serialize_task(self) -> str:
        """The reverse of resolve_callable, transform callable into dotted notation"""
        return MODULE_METHOD_DEL.join([self.fn.__module__, self.fn.__qualname__])

    def get_callable(self) -> DispatcherCallable:
        if inspect.isclass(self.fn):
            # the callable is a class, e.g., RunJob; instantiate and
            # return its `run()` method
            return self.fn().run

        return self.fn

    def publication_defaults(self) -> dict:
        defaults = {'task': self.serialize_task(), 'time_pub': time.time()}
        if self.on_duplicate not in [DuplicateBehavior.parallel.value, None]:
            defaults['on_duplicate'] = self.on_duplicate
        return defaults

    def delay(self, *args, **kwargs):
        return self.apply_async(args, kwargs)

    def get_async_body(self, args=None, kwargs=None, uuid=None, delay=None, **kw):
        """
        Get the python dict to become JSON data in the pg_notify message
        This same message gets passed over the dispatcher IPC queue to workers
        If a task is submitted to a multiprocessing pool, skipping pg_notify, this might be used directly
        """
        body = self.publication_defaults()
        body.update({'uuid': uuid or str(uuid4()), 'args': args or [], 'kwargs': kwargs or {}})

        if delay is not None:
            body['delay'] = delay

        # TODO: callback to add other things, guid in case of AWX

        body.update(**kw)
        return body

    def apply_async(self, args=None, kwargs=None, queue=None, uuid=None, delay=None, connection=None, config=None, on_duplicate=None, **kw):
        queue = queue or self.queue
        if not queue:
            msg = f'{self.name}: Queue value required and may not be None'
            logger.error(msg)
            raise ValueError(msg)

        if callable(queue):
            queue = queue()

        obj = self.get_async_body(args=args, kwargs=kwargs, uuid=uuid, delay=delay, **kw)
        if on_duplicate:
            obj['on_duplicate'] = on_duplicate

        # TODO: before sending, consult an app-specific callback if configured
        from dispatcher.brokers.pg_notify import publish_message

        # NOTE: the kw will communicate things in the database connection data
        publish_message(queue, json.dumps(obj), connection=connection, config=config, **kw)
        return (obj, queue)


class UnregisteredMethod(DispatcherMethod):
    def __init__(self, task: str):
        fn = resolve_callable(task)
        if fn is None:
            raise ImportError(f'Dispatcher could not import provided identifier: {task}')
        super().__init__(fn)


class DispatcherMethodRegistry:
    def __init__(self) -> None:
        self.registry: Set[DispatcherMethod] = set()
        self.lock = threading.Lock()
        self._lookup_dict: dict[str, DispatcherMethod] = {}
        self._registration_closed: bool = False

    def register(self, fn, **kwargs) -> DispatcherMethod:
        if self._registration_closed:
            self._lookup_dict = {}
            self._registration_closed = False

        with self.lock:
            dmethod = DispatcherMethod(fn, **kwargs)
            self.registry.add(dmethod)
        return dmethod

    @property
    def lookup_dict(self) -> dict[str, DispatcherMethod]:
        "Any reference to the lookup_dict will close registration"
        if not self._registration_closed:
            self._registration_closed = True
            for dmethod in self.registry:
                self._lookup_dict[dmethod.serialize_task()] = dmethod
        return self._lookup_dict

    def get_method(self, task: str, allow_unregistered: bool = True) -> DispatcherMethod:
        if task in self.lookup_dict:
            return self.lookup_dict[task]

        if allow_unregistered:
            return UnregisteredMethod(task)

        raise NotRegistered(f'Provided method {task} is unregistered and this is not allowed')

    def get_from_callable(self, fn: DispatcherCallable) -> DispatcherMethod:
        for dmethod in self.registry:
            if dmethod.fn is fn:
                return dmethod
        raise RuntimeError(f'Callable {fn} does not appear to be registered')


registry = DispatcherMethodRegistry()
