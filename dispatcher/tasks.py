import asyncio
import logging
from typing import Optional

from dispatcher.factories import get_publisher_from_settings
from dispatcher.publish import task
from dispatcher.registry import control_registry

logger = logging.getLogger(__name__)


@task()
def reply_to_control(reply_channel: str, message: str):
    broker = get_publisher_from_settings()
    broker.publish_message(channel=reply_channel, message=message)


def task_filter_match(pool_task: dict, msg_data: dict) -> bool:
    """The two messages are functionally the same or not"""
    filterables = ('task', 'args', 'kwargs', 'uuid')
    for key in filterables:
        expected_value = msg_data.get(key)
        if expected_value:
            if pool_task.get(key, None) != expected_value:
                return False
    return True


async def _find_tasks(dispatcher, cancel: bool = False, **data) -> list[tuple[Optional[str], dict]]:
    "Utility method used for both running and cancel control methods"
    ret = []
    for worker in dispatcher.pool.workers.values():
        if worker.current_task:
            if task_filter_match(worker.current_task, data):
                if cancel:
                    logger.warning(f'Canceling task in worker {worker.worker_id}, task: {worker.current_task}')
                    worker.cancel()
                ret.append((worker.worker_id, worker.current_task))
    for message in dispatcher.pool.queued_messages:
        if task_filter_match(message, data):
            if cancel:
                logger.warning(f'Canceling task in pool queue: {message}')
                dispatcher.pool.queued_messages.remove(message)
            ret.append((None, message))
    for capsule in dispatcher.delayed_messages.copy():
        if task_filter_match(capsule.message, data):
            if cancel:
                logger.warning(f'Canceling delayed task (uuid={capsule.uuid})')
                capsule.task.cancel()
                try:
                    await capsule.task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.error(f'Error canceling delayed task (uuid={capsule.uuid})')
                dispatcher.delayed_messages.remove(capsule)
            ret.append(('<delayed>', capsule.message))
    return ret


def control_task(fn):
    control_registry.register(fn)


# TODO: hold pool management lock for these things
@control_task
async def running(dispatcher, **data) -> list[tuple[Optional[str], dict]]:
    # TODO: include delayed tasks in results
    return await _find_tasks(dispatcher, **data)


@control_task
async def cancel(dispatcher, **data) -> list[tuple[Optional[str], dict]]:
    # TODO: include delayed tasks in results
    return await _find_tasks(dispatcher, cancel=True, **data)


@control_task
async def alive(dispatcher, **data):
    return


@control_task
async def workers(dispatcher, **data):
    ret = []
    for worker in dispatcher.pool.workers.values():
        ret.append(worker.get_data())
    return ret
