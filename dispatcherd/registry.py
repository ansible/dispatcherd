import inspect
import json
import logging
import threading
import time
from typing import Callable, Optional, Set, Tuple, Union
from uuid import uuid4

from .config import LazySettings
from .config import settings as global_settings
from .utils import MODULE_METHOD_DELIMITER, DispatcherCallable, resolve_callable

logger = logging.getLogger(__name__)


class DispatcherError(RuntimeError):
    pass


class NotRegistered(DispatcherError):
    pass


class InvalidMethod(DispatcherError):
    pass


class DispatcherMethod:

    def __init__(self, fn: DispatcherCallable, queue: Optional[Union[Callable, str]] = None, bind: bool = False, **submission_defaults) -> None:
        if not hasattr(fn, '__qualname__'):
            raise InvalidMethod('Can only register methods and classes')
        self.fn = fn
        self.submission_defaults = submission_defaults or {}
        self.queue = queue  # applied by worker, not publisher
        self.bind = bind  # only needed to submit, do not need to pass in message

    def serialize_task(self) -> str:
        """The reverse of resolve_callable, transform callable into dotted notation"""
        return MODULE_METHOD_DELIMITER.join([self.fn.__module__, self.fn.__qualname__])

    def get_callable(self) -> Callable:
        if inspect.isclass(self.fn):
            # the callable is a class, e.g., RunJob; instantiate and
            # return its `run()` method
            return self.fn().run

        return self.fn

    def publication_defaults(self) -> dict:
        defaults = {}
        for k, v in self.submission_defaults.items():
            if v:  # all None or falsy values have no effect
                defaults[k] = v
        defaults['task'] = self.serialize_task()
        defaults['time_pub'] = time.time()
        return defaults

    def delay(self, *args, **kwargs) -> Tuple[dict, str]:
        return self.apply_async(args, kwargs)

    def get_async_body(
        self, args=None, kwargs=None, uuid=None, bind: bool = False, on_duplicate: Optional[str] = None, timeout: Optional[float] = 0.0, delay: float = 0.0
    ) -> dict:
        """
        Get the python dict to become JSON data in the pg_notify message
        This same message gets passed over the dispatcher IPC queue to workers
        If a task is submitted to a multiprocessing pool, skipping pg_notify, this might be used directly
        """
        body = self.publication_defaults()
        # These params are forced to be set on every submission, can not be generic to task
        body.update({'uuid': uuid or str(uuid4()), 'args': args or [], 'kwargs': kwargs or {}})

        # TODO: callback to add other things, guid in case of AWX

        if bind:  # only needed for testing
            body['bind'] = bind
        if on_duplicate:
            body['on_duplicate'] = on_duplicate
        if delay:
            body['delay'] = delay
        if timeout:
            body['timeout'] = timeout

        return body

    def apply_async(self, args=None, kwargs=None, queue=None, uuid=None, settings: LazySettings = global_settings, **kw) -> Tuple[dict, str]:
        queue = queue or self.queue

        if callable(queue):
            queue = queue()

        obj = self.get_async_body(args=args, kwargs=kwargs, uuid=uuid, **kw)

        from dispatcherd.factories import get_publisher_from_settings

        broker = get_publisher_from_settings(settings=settings)

        # TODO: exit if a setting is applied to disable publishing

        broker.publish_message(channel=queue, message=json.dumps(obj))
        return (obj, queue)


class UnregisteredMethod(DispatcherMethod):
    def __init__(self, task: str) -> None:
        fn = resolve_callable(task)
        if fn is None:
            raise ImportError(f'Dispatcherd could not import provided identifier: {task}')
        super().__init__(fn)


class DispatcherMethodRegistry:
    def __init__(self) -> None:
        self.registry: Set[DispatcherMethod] = set()
        self.lock = threading.Lock()
        self._lookup_dict: dict[str, DispatcherMethod] = {}
        self._registration_closed: bool = False

    def register(self, fn, **kwargs) -> DispatcherMethod:
        with self.lock:
            if self._registration_closed:
                self._lookup_dict = {}
                self._registration_closed = False
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

        # Creating UnregisteredMethod will import the method
        unregistered_candidate = UnregisteredMethod(task)

        # If this import had a side effect of registering the method, then update ourselves
        if task in self.lookup_dict:
            return self.lookup_dict[task]

        if allow_unregistered:
            return unregistered_candidate

        raise NotRegistered(f'Provided method {task} is unregistered and this is not allowed')

    def get_from_callable(self, fn: DispatcherCallable) -> DispatcherMethod:
        for dmethod in self.registry:
            if dmethod.fn is fn:
                return dmethod
        raise RuntimeError(f'Callable {fn} does not appear to be registered')


registry = DispatcherMethodRegistry()
