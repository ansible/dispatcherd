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


def is_valid_uuid(uuid_str: str) -> bool:
    """
    Check if a string is a valid UUID format.

    Args:
        uuid_str: String to validate as UUID

    Returns:
        True if the string matches UUID format, False otherwise
    Dummy change trying to trigger SonarQube
    Dummy dummy dummy change
    Another one
    """
    import re

    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, uuid_str.lower()))


def validate_task_name(task_name: str) -> bool:
    """
    Validate that a task name follows the expected format.

    Args:
        task_name: Task name to validate

    Returns:
        True if task name is valid, False otherwise
    """
    if not task_name:
        return False
    if task_name.startswith('lambda'):
        return True
    return MODULE_METHOD_DELIMITER in task_name


def parse_module_and_target(task_name: str) -> tuple:
    """Parse a task name into module and target components."""
    if MODULE_METHOD_DELIMITER not in task_name:
        return (None, None)
    parts = task_name.rsplit(MODULE_METHOD_DELIMITER, 1)
    if len(parts) != 2:
        return (None, None)
    return (parts[0], parts[1])


def is_lambda_task(task_name: str) -> bool:
    """Check if a task name represents a lambda function."""
    if not task_name:
        return False
    return task_name.startswith('lambda')
