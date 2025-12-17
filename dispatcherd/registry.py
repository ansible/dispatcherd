import inspect
import json
import logging
import threading
import time
from typing import Callable, Iterable, Optional, Set, Tuple
from uuid import uuid4

from .config import LazySettings
from .config import settings as global_settings
from .protocols import ProcessorParams
from .utils import MODULE_METHOD_DELIMITER, DispatcherCallable, resolve_callable

logger = logging.getLogger(__name__)


class DispatcherError(RuntimeError):
    pass


class NotRegistered(DispatcherError):
    pass


class InvalidMethod(DispatcherError):
    pass


class DispatcherMethod:

    def __init__(
        self,
        fn: DispatcherCallable,
        queue: Callable | str | None = None,
        bind: bool = False,
        processor_options: Iterable[ProcessorParams] = (),
        **submission_defaults,
    ) -> None:
        """Class that tracks a registered method to be ran by dispatcherd

        Parameters:
            fn: the python method to register, or class with .run method
            queue: default queue specific to this task
                this might be used to broadcast certain tasks to multiple services
                or send certain tasks to certain groups of workers.
                This can be over-ridden on task submission.
                Takes precedence over the broker default queue.
            bind: method expects a first argument to be the dispatcherd interaction object
                See DispatcherBoundMethods for what this offers
        """
        if not hasattr(fn, '__qualname__'):
            raise InvalidMethod('Can only register methods and classes')
        self.fn = fn
        self.submission_defaults = submission_defaults or {}
        self.queue = queue  # If null, method expects queue from broker default or submitter
        self.bind = bind  # only needed to submit, do not need to pass in message
        self.processor_options = processor_options

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
        for part in self.processor_options:
            defaults.update(part.to_dict())
        return defaults

    def delay(self, *args, **kwargs) -> Tuple[dict, str]:
        return self.apply_async(args=args, kwargs=kwargs)

    def get_async_body(
        self,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        uuid: Optional[str] = None,
        bind: bool = False,
        timeout: Optional[float] = 0.0,
        processor_options: Iterable[ProcessorParams] = (),
    ) -> dict:
        """
        Get the python dict to become JSON data in the pg_notify message
        This same message gets passed over the dispatcher IPC queue to workers
        If a task is submitted to a multiprocessing pool, skipping pg_notify, this might be used directly
        """
        body = self.publication_defaults()
        # These params are forced to be set on every submission, can not be generic to task
        body['uuid'] = uuid or str(uuid4())

        if args:
            body['args'] = args
        if kwargs:
            body['kwargs'] = kwargs
        # The bind param in the submission data is only needed for testing
        # normally this should be applied on the task decorator
        if bind:
            body['bind'] = bind
        if timeout:
            body['timeout'] = timeout

        for part in processor_options:
            body.update(part.to_dict())

        return body

    def apply_async(
        self,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        queue: str | Callable[..., str] | None = None,
        uuid: Optional[str] = None,
        settings: LazySettings = global_settings,
        bind: bool = False,
        timeout: Optional[float] = 0.0,
        processor_options: Iterable[ProcessorParams] = (),
    ) -> Tuple[dict, str]:
        """Submit a task to be ran by dispatcherd worker(s)

        This submission does not provide confirmation.
        If you need confirmation from the service, look into the "run" control command.
        """

        resolved_queue: Optional[str]
        if queue and callable(queue):
            resolved_queue = queue()
        else:
            resolved_queue = queue  # Can still be None if we rely on the broker default channel

        obj = self.get_async_body(args=args, kwargs=kwargs, uuid=uuid, bind=bind, timeout=timeout, processor_options=processor_options)

        from dispatcherd.factories import get_publisher_from_settings

        broker = get_publisher_from_settings(settings=settings)

        # The broker itself has a channel default, so we return that if applicable
        used_queue = broker.publish_message(channel=resolved_queue, message=json.dumps(obj))
        return (obj, used_queue)


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
        raise NotRegistered(f'Callable {fn} does not appear to be registered')


registry = DispatcherMethodRegistry()
