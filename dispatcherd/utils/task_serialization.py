"""Helpers for importing and describing dispatcherd tasks.

The functions in this module cover both directions of the "dotted path" task
notation that dispatcherd uses when communicating over brokers:

* Use :func:`resolve_callable` to import and return the callable a task string
  refers to.
* Use :func:`serialize_task` to turn a callable back into its dotted string
  form.

The :class:`DuplicateBehavior` enum documents the policies exposed to users for
handling duplicate tasks.
"""

import importlib
from enum import Enum
from typing import Callable, Optional, Protocol, Type, Union, runtime_checkable

MODULE_METHOD_DELIMITER = '.'


@runtime_checkable
class RunnableClass(Protocol):
    """Simplified protocol for objects that offer a ``run`` method.

    Dispatcherd commonly references callables that either are plain functions or
    classes with a ``run`` method (similar to Celery tasks).  This protocol lets
    type checkers know that instances implementing ``run`` are acceptable when
    resolving task references.
    """

    def run(self, *args, **kwargs) -> None: ...


DispatcherCallable = Union[Callable, Type[RunnableClass]]


def resolve_callable(task: str) -> Optional[Callable]:
    """Import and return the callable identified by a dotted task string.

    Examples
    --------
    ``awx.main.tasks.system.delete_inventory``
        -> ``awx.main.tasks.system.delete_inventory`` function

    ``awx.main.tasks.jobs.RunProjectUpdate``
        -> ``RunProjectUpdate`` class (which exposes ``run``)

    Parameters
    ----------
    task:
        Dotted notation describing the module and attribute to import.

    Returns
    -------
    Callable | None
        The callable object, or ``None`` if the attribute does not exist.
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


def serialize_task(func: Callable) -> str:
    """Return the dotted task notation for ``func``."""
    return MODULE_METHOD_DELIMITER.join([func.__module__, func.__name__])


class DuplicateBehavior(Enum):
    """Policies available for handling duplicate tasks.

    This enum corresponds to the ``on_duplicate`` values used by the
    :class:`dispatcherd.processors.blocker.Blocker.Params` dataclass.  Each
    member describes when a newly submitted task should be delayed or discarded
    if another task with the same identifying fields is already queued or
    running.
    """

    parallel = 'parallel'  # run multiple versions of same task at same time
    discard = 'discard'  # if task is submitted twice, discard the 2nd one
    serial = 'serial'  # hold duplicate submissions in queue but only run 1 at a time
    queue_one = 'queue_one'  # hold only 1 duplicate submission in queue, discard any more
