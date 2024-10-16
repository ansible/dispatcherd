import importlib


def resolve_callable(task):
    """
    Transform a dotted notation task into an imported, callable function, e.g.,

    awx.main.tasks.system.delete_inventory
    awx.main.tasks.jobs.RunProjectUpdate

    In AWX this also did validation that the method was marked as a task.
    That is out of scope of this method now.
    This is mainly used by the worker.
    """
    if task.startswith('lambda:'):
        return eval(task)

    module, target = task.rsplit('.', 1)
    module = importlib.import_module(module)
    _call = None
    if hasattr(module, target):
        _call = getattr(module, target, None)

    return _call


def serialize_task(f) -> str:
    """The reverse of resolve_callable, transform callable into dotted notation"""
    return '.'.join([f.__module__, f.__name__])
