import importlib
from enum import Enum
from typing import Callable, Optional, Protocol, Type, Union, runtime_checkable


@runtime_checkable
class RunnableClass(Protocol):
    def run(self, *args, **kwargs) -> None: ...


MODULE_METHOD_DELIMITER = '.'


DispatcherCallable = Union[Callable, Type[RunnableClass]]


def resolve_callable(task: str) -> Optional[Callable]:
    """
    Transform a dotted notation task into an imported, callable function, e.g.,

    awx.main.tasks.system.delete_inventory
    awx.main.tasks.jobs.RunProjectUpdate

    In AWX this also did validation that the method was marked as a task.
    That is out of scope of this method now.
    This is mainly used by the worker.
    """
    if task.startswith('lambda'):
        return eval(task)

    if MODULE_METHOD_DELIMITER not in task:
        raise RuntimeError(f'Given task name can not be parsed as task {task}')

    module_name, target = task.rsplit(MODULE_METHOD_DELIMITER, 1)
    module = importlib.import_module(module_name)
    _call = None
    if hasattr(module, target):
        _call = getattr(module, target, None)

    return _call


def serialize_task(f: Callable) -> str:
    """The reverse of resolve_callable, transform callable into dotted notation"""
    return MODULE_METHOD_DELIMITER.join([f.__module__, f.__name__])


class DuplicateBehavior(Enum):
    parallel = 'parallel'  # run multiple versions of same task at same time
    discard = 'discard'  # if task is submitted twice, discard the 2nd one
    serial = 'serial'  # hold duplicate submissions in queue but only run 1 at a time
    queue_one = 'queue_one'  # hold only 1 duplicate submission in queue, discard any more
