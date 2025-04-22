import json
import logging
import multiprocessing
from queue import Empty as QueueEmpty
from typing import Type, cast

from ..config import is_setup
from ..config import settings as global_settings
from ..config import setup
from ..protocols import TaskWorker
from ..utils import resolve_callable
from .exceptions import DispatcherCancel

logger = logging.getLogger(__name__)


"""This module contains the target passed to multiprocessing.Process"""


def work_loop_internal(worker: TaskWorker) -> None:
    """
    The forever loop for dispatcherd workers after all objects have been initialized
    """
    worker.on_start()
    worker.finished_queue.put(worker.get_ready_message())

    while not worker.should_exit():
        try:
            if worker.idle_timeout:
                message = worker.message_queue.get(timeout=worker.idle_timeout)
            else:
                message = worker.message_queue.get()
        except DispatcherCancel:
            logger.info(f'Worker {worker.worker_id} received a task cancel signal in main loop, ignoring')
            continue
        except QueueEmpty:
            logger.debug(f'Worker {worker.worker_id} QueueEmpty condition, probably from timeout')
            worker.on_idle()
            continue  # a race condition that mostly can be ignored
        except Exception as exc:
            logger.exception(f"Exception on worker {worker.worker_id}, type {type(exc)}, exiting")
            break

        if message == "stop":
            logger.warning(f"Worker {worker.worker_id} exiting main loop due to stop message.")
            break

        worker.pre_task(message)
        result = worker.perform_work(message)
        worker.post_task(result)

        # Indicate that the task is finished by putting a message in the finished_queue
        worker.finished_queue.put(result)

    worker.on_shutdown()
    worker.finished_queue.put(worker.get_shutdown_message())
    logger.debug(f'Worker {worker.worker_id} informed the pool manager that we have exited')


def work_loop(worker_id: int, settings: str, finished_queue: multiprocessing.Queue, message_queue: multiprocessing.Queue) -> None:
    """
    Worker function that processes messages from the queue and sends confirmation
    to the finished_queue once done.
    """
    # Load settings passed from parent
    # this assures that workers are all configured the same
    # If user configured workers via preload_modules, do nothing here
    if not is_setup():
        config = json.loads(settings)
        dispatcher_settings = setup(config=config)
    else:
        logger.debug(f'Not calling setup() for worker_id={worker_id} because environment is already configured')
        dispatcher_settings = global_settings

    worker_cls = dispatcher_settings.worker.get('worker_cls', 'dispatcherd.worker.task.TaskWorker')
    worker_kwargs = dispatcher_settings.worker.get('worker_kwargs', {})
    cls = cast(Type[TaskWorker], resolve_callable(worker_cls))
    assert cls is not None

    worker = cls(worker_id=worker_id, finished_queue=finished_queue, message_queue=message_queue, **worker_kwargs)

    work_loop_internal(worker)
