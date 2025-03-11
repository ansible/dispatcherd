import asyncio
import io
import logging

__all__ = ['running', 'cancel', 'alive', 'aio_tasks', 'workers']


logger = logging.getLogger(__name__)


def task_filter_match(pool_task: dict, msg_data: dict) -> bool:
    """The two messages are functionally the same or not"""
    filterables = ('task', 'args', 'kwargs', 'uuid')
    for key in filterables:
        expected_value = msg_data.get(key)
        if expected_value:
            if pool_task.get(key, None) != expected_value:
                return False
    return True


async def _find_tasks(dispatcher, cancel: bool = False, **data) -> dict[str, dict]:
    "Utility method used for both running and cancel control methods"
    ret = {}
    for worker in dispatcher.pool.workers.values():
        if worker.current_task:
            if task_filter_match(worker.current_task, data):
                if cancel:
                    logger.warning(f'Canceling task in worker {worker.worker_id}, task: {worker.current_task}')
                    worker.cancel()
                ret[f'worker-{worker.worker_id}'] = worker.current_task
    for i, message in enumerate(dispatcher.pool.blocker.blocked_messages):
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool blocker: {message}')
                dispatcher.pool.blocker.blocked_messages.remove(message)
            ret[f'blocked-{i}'] = message
    for i, message in enumerate(dispatcher.pool.queuer.queued_messages):
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool queue: {message}')
                dispatcher.pool.queuer.queued_messages.remove(message)
            ret[f'queued-{i}'] = message
    for i, capsule in enumerate(dispatcher.delayed_messages.copy()):
        if task_filter_match(capsule.message, data):
            if cancel:
                uuid = capsule.message.get('uuid', '<unknown>')
                logger.warning(f'Canceling delayed task (uuid={uuid})')
                capsule.has_ran = True  # make sure we do not run by accident
                dispatcher.delayed_messages.remove(capsule)
            ret[f'delayed-{i}'] = capsule.message
    return ret


async def running(dispatcher, **data) -> dict[str, dict]:
    async with dispatcher.pool.management_lock:
        return await _find_tasks(dispatcher, **data)


async def cancel(dispatcher, **data) -> dict[str, dict]:
    async with dispatcher.pool.management_lock:
        return await _find_tasks(dispatcher, cancel=True, **data)


def _stack_from_task(task: asyncio.Task, limit=6) -> str:
    buffer = io.StringIO()
    task.print_stack(file=buffer, limit=limit)
    return buffer.getvalue()


async def aio_tasks(dispatcher, **data) -> dict[str, dict]:
    ret = {}
    extra = {}
    if 'limit' in data:
        extra['limit'] = data['limit']

    for task in asyncio.all_tasks():
        task_name = task.get_name()
        ret[task_name] = {'done': task.done(), 'stack': _stack_from_task(task, **extra)}
    return ret


async def alive(dispatcher, **data) -> dict:
    return {}


async def workers(dispatcher, **data) -> dict:
    ret = {}
    for worker in dispatcher.pool.workers.values():
        ret[f'worker-{worker.worker_id}'] = worker.get_data()
    return ret
