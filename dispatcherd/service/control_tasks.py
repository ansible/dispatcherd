import asyncio
import io
import logging

from ..protocols import DispatcherMain

__all__ = ['running', 'cancel', 'alive', 'aio_tasks', 'workers', 'producers', 'metrics', 'main', 'status', 'chunks', 'set_log_level']


logger = logging.getLogger(__name__)
DISPATCHER_LOGGER_NAME = 'dispatcherd'
dispatcherd_logger = logging.getLogger(DISPATCHER_LOGGER_NAME)


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

    Control Args:
        task:
            type: str
            help: Limit the results to a task name (exact match).
        uuid:
            type: str
            help: Limit the results to a specific task uuid.
    """
    async with dispatcher.pool.workers.management_lock:
        return await _find_tasks(dispatcher=dispatcher, data=data)


async def cancel(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Cancel all tasks that match the filter given by data

    The protocol for the data filtering is the same as the running command.

    Control Args:
        task:
            type: str
            help: Cancel only tasks matching this task name.
        uuid:
            type: str
            help: Cancel only tasks with this uuid.
    """
    async with dispatcher.pool.workers.management_lock:
        return await _find_tasks(dispatcher=dispatcher, cancel=True, data=data)


def _stack_from_task(task: asyncio.Task, limit: int = 6) -> str:
    buffer = io.StringIO()
    task.print_stack(file=buffer, limit=limit)
    return buffer.getvalue()


async def aio_tasks(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Information on the asyncio tasks running in the dispatcher main process

    Control Args:
        limit:
            type: int
            help: Optional stack depth when printing asyncio task traces.
    """
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


async def metrics(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information about the metrics server, if enabled"""
    metrics_server = getattr(dispatcher, 'metrics', None)
    if metrics_server is None:
        return {'enabled': False}

    return {'enabled': True, 'status': metrics_server.get_status_data()}


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


async def chunks(dispatcher: DispatcherMain, data: dict) -> dict[str, dict]:
    """Return chunk accumulator diagnostics and buffered message metadata."""
    partials = await dispatcher.chunk_accumulator.aget_partial_messages()
    return {
        'status': dispatcher.chunk_accumulator.get_status_data(),
        'partials': partials,
    }


async def status(dispatcher: DispatcherMain, data: dict) -> dict:
    """Information from all other non-destructive commands nested in a sub-dictionary"""
    ret = {}
    for command in __all__:
        if command in ('cancel', 'alive', 'status', 'run', 'set_log_level'):
            continue
        control_method = globals()[command]
        ret[command] = await control_method(dispatcher=dispatcher, data={})
    return ret


def _coerce_log_level(level_name: str | None) -> int | None:
    """Normalize level string into (level value, normalized name)."""
    if not level_name:
        return None
    normalized = level_name.strip().upper()
    if not normalized:
        return None

    # newer python API - get mapping
    level_map = logging.getLevelNamesMapping()
    resolved_level = level_map.get(normalized)
    if resolved_level is None:
        return None

    return resolved_level


async def set_log_level(dispatcher: DispatcherMain, data: dict) -> dict[str, str]:
    """Set the active log level for the `dispatcherd` logger in the main process.

    Control Args:
        level:
            type: str
            required: true
            choices: [DEBUG, INFO, WARNING, ERROR, CRITICAL]
            help: Desired log level for the dispatcherd logger.
    """
    requested_level = data.get('level')
    level_value: int | None
    if type(requested_level) is int:
        # booleans are technically integers and we do not want to accept those
        level_value = requested_level
    elif isinstance(requested_level, str):
        level_value = _coerce_log_level(requested_level)
        if level_value is None:
            return {'error': f"Unknown log level '{requested_level}'."}
    else:
        return {'error': 'Log level must be provided as a string or int via the "level" key.'}

    previous_level = logging.getLevelName(dispatcherd_logger.level)
    dispatcherd_logger.setLevel(level_value)
    new_level = logging.getLevelName(dispatcherd_logger.level)
    logger.info("Changed %s logger level from %s to %s", DISPATCHER_LOGGER_NAME, previous_level, new_level)

    return {
        'logger': DISPATCHER_LOGGER_NAME,
        'level': new_level,
        'previous_level': previous_level,
    }
