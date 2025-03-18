import asyncio
import io
import logging

from ..protocols import DispatcherMain

__all__ = ['running', 'cancel', 'alive', 'aio_tasks', 'workers', 'producers', 'main', 'status']


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


async def _find_tasks(dispatcher: DispatcherMain, data: dict, cancel: bool = False) -> dict[str, dict]:
    "Utility method used for both running and cancel control methods"
    ret = {}
    for worker in dispatcher.pool.workers:
        if worker.current_task:
            if task_filter_match(worker.current_task, data):
                if cancel:
                    logger.warning(f'Canceling task in worker {worker.worker_id}, task: {worker.current_task}')
                    worker.cancel()
                ret[f'worker-{worker.worker_id}'] = worker.current_task
    for i, message in enumerate(dispatcher.pool.blocker):
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool blocker: {message}')
                dispatcher.pool.blocker.remove_task(message)
            ret[f'blocked-{i}'] = message
    for i, message in enumerate(dispatcher.pool.queuer):
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool queue: {message}')
                dispatcher.pool.queuer.remove_task(message)
            ret[f'queued-{i}'] = message
    for i, capsule in enumerate(list(dispatcher.delayer)):
        if task_filter_match(capsule.message, data):
            if cancel:
                uuid = capsule.message.get('uuid', '<unknown>')
                logger.warning(f'Canceling delayed task (uuid={uuid})')
                capsule.has_ran = True  # make sure we do not run by accident
                dispatcher.delayer.remove_capsule(capsule)
            ret[f'delayed-{i}'] = capsule.message
    return ret


async def running(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Information on running tasks managed by this dispatcherd service

    Data may be used to filter the tasks of interest.
    Keys and values in data correspond to expected key-values in the message,
    but are limited to task, kwargs, args, and uuid.
    """
    async with dispatcher.pool.workers.management_lock:
        return await _find_tasks(dispatcher=dispatcher, data=data)


async def cancel(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Cancel all tasks that match the filter given by data

    The protocol for the data filtering is the same as the running command.
    """
    async with dispatcher.pool.workers.management_lock:
        return await _find_tasks(dispatcher=dispatcher, cancel=True, data=data)


def _stack_from_task(task: asyncio.Task, limit: int = 6) -> str:
    buffer = io.StringIO()
    task.print_stack(file=buffer, limit=limit)
    return buffer.getvalue()


async def aio_tasks(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Information on the asyncio tasks running in the dispatcher main process"""
    ret = {}
    extra = {}
    if 'limit' in data:
        extra['limit'] = data['limit']

    for task in asyncio.all_tasks():
        task_name = task.get_name()
        ret[task_name] = {'done': task.done(), 'stack': _stack_from_task(task, **extra)}
    return ret


async def alive(dispatcher: DispatcherMain, data: dict) -> dict:
    """Returns no information, used to get fast roll-call of instances"""
    return {}


async def workers(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information about subprocess workers"""
    ret = {}
    for worker in dispatcher.pool.workers:
        ret[f'worker-{worker.worker_id}'] = worker.get_status_data()
    return ret


async def producers(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information about the enabled task producers"""
    ret = {}
    for producer in dispatcher.producers:
        ret[str(producer)] = producer.get_status_data()
    return ret


async def run(dispatcher: DispatcherMain, data: dict) -> dict:
    """Run a task. The control data should follow the standard message protocol.

    You could just submit task data, as opposed to submitting a control task
    with task data nested in control_data, which is what this is.
    This might be useful if you:
    - need to get a confirmation that your task has been received
    - you need to start a task from another task
    """
    for producer in dispatcher.producers:
        if hasattr(producer, 'submit_task'):
            try:
                await producer.submit_task(data)
            except Exception as exc:
                return {'error': str(exc)}
            return {'ack': data}
    return {'error': 'A ControlProducer producer is not enabled. Add it to the list of producers in the service config to use this.'}


async def main(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information about scalar quantities on the main or pool objects"""
    ret = dispatcher.get_status_data()
    ret["pool"] = dispatcher.pool.get_status_data()
    return ret


async def status(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information from all other non-destructive commands nested in a sub-dictionary"""
    ret = {}
    for command in __all__:
        if command in ('cancel', 'alive', 'status', 'run'):
            continue
        control_method = globals()[command]
        ret[command] = await control_method(dispatcher=dispatcher, data={})
    return ret
