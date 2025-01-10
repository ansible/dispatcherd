import threading
import inspect
import time

from typing import Callable, Optional, Set, Protocol, runtime_checkable, Union

from dispatcher.constants import MODULE_METHOD_DEL
from dispatcher.utils import resolve_callable


class DispatcherError(RuntimeError):
    pass


class RegistrationClosed(DispatcherError):
    pass


class NotRegistered(DispatcherError):
    pass


@runtime_checkable
class RunnableClass(Protocol):
    def run(self, *args, **kwargs) -> None:
        ...


DispatcherCallable = Union[Callable, RunnableClass]


class MethodRun:
    def __init__(self, message: dict):
        self.published: Optional[float] = None
        self.received: Optional[float] = None
        self.dispatched: Optional[float] = None
        self.finished: Optional[float] = None


class DispatcherMethod:
    def __init__(self, fn: DispatcherCallable):
        self.fn = fn
        self.call_ct: int = 0
        self.runtime: float = 0.0

    def serialize_task(self) -> str:
        """The reverse of resolve_callable, transform callable into dotted notation"""
        return MODULE_METHOD_DEL.join([self.fn.__module__, self.fn.__qualname__])

    def get_callable(self) -> Callable:
        if inspect.isclass(self.fn):
            # the callable is a class, e.g., RunJob; instantiate and
            # return its `run()` method
            return self.fn().run

        return self.fn

    def publication_data(self) -> dict:
        return {'task': self.serialize_task(), 'time_pub': time.time()}


class UnregisteredMethod:
    def __init__(self, task: str):
        super().__init__(resolve_callable(task))


class DispatcherMethodRegistry:
    def __init__(self):
        self.registry: Set[DispatcherMethod] = set()
        self.lock = threading.Lock()
        self._lookup_dict: dict[str, DispatcherMethod] = {}
        self._registration_closed: bool = False

    def register(self, fn) -> DispatcherMethod:
        if self._registration_closed:
            raise RegistrationClosed('Can not register new methods after import')
        with self.lock:
            dmethod = DispatcherMethod(fn)
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

    def get_method(self, task: str, allow_unregistered: bool= True) -> DispatcherMethod:
        if task in self.lookup_dict:
            return self.lookup_dict[task]

        if allow_unregistered:
            return UnregisteredMethod(task)

        raise NotRegistered(f'Provided method {task} is unregistered and this is not allowed')


registry = DispatcherMethodRegistry()
