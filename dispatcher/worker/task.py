import json
import logging
import multiprocessing
import os
import signal
import sys
import time
import traceback
from queue import Empty as QueueEmpty

from ..config import setup
from ..registry import DispatcherMethodRegistry
from ..registry import registry as global_registry

logger = logging.getLogger(__name__)


"""This module contains code ran by the worker subprocess"""


class DispatcherCancel(Exception):
    pass


class WorkerSignalHandler:
    def __init__(self, worker_id):
        self.kill_now = False
        self.worker_id = worker_id
        signal.signal(signal.SIGUSR1, self.task_cancel)
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def task_cancel(self, *args, **kwargs):
        raise DispatcherCancel

    def exit_gracefully(self, *args, **kwargs):
        logger.info(f'Worker {self.worker_id} received worker process exit signal')
        self.kill_now = True


class DispatcherBoundMethods:
    """
    If you use the task decorator with the bind=True argument,
    an object of this type will be passed in.
    This contains public methods for users of the dispatcher to call.
    """

    def __init__(self, worker_id, message):
        self.worker_id = worker_id
        self.uuid = message.get('uuid', '<unknown>')


class TaskWorker:
    """
    A worker implementation that deserializes task messages and runs native
    Python code.

    This mainly takes messages from the main process, imports, and calls them.

    Original code existed at:
    https://github.com/ansible/awx/blob/devel/awx/main/dispatch/worker/task.py
    https://github.com/ansible/awx/blob/devel/awx/main/dispatch/worker/base.py

    Major change from AWX is adding __init__ which now runs post-fork.
    Previously this initialized pre-fork, making init logic unusable.
    """

    def __init__(self, worker_id: int, registry: DispatcherMethodRegistry = global_registry):
        self.worker_id: int = worker_id
        self.registry = registry
        self.ppid = os.getppid()
        self.pid = os.getpid()
        self.signal_handler = WorkerSignalHandler(worker_id)

    def should_exit(self) -> bool:
        """Called before continuing the loop, something suspicious, return True, should exit"""
        if os.getppid() != self.ppid:
            logger.error(f'Worker {self.worker_id}, my parent PID changed, this process has been orphaned, like segfault or sigkill, exiting')
            return True
        elif self.signal_handler.kill_now:
            return True
        return False

    def get_uuid(self, message):
        return message.get('uuid', '<unknown>')

    def produce_binder(self, message: dict) -> DispatcherBoundMethods:
        """
        Return the object with public callbacks to pass to the task
        """
        return DispatcherBoundMethods(self.worker_id, message)

    def run_callable(self, message):
        """
        Import the Python code and run it.
        """
        task = message['task']
        args = message.get('args', []).copy()
        kwargs = message.get('kwargs', {})
        dmethod = self.registry.get_method(task)
        _call = dmethod.get_callable()

        # don't print kwargs, they often contain launch-time secrets
        logger.debug(f'task (uuid={self.get_uuid(message)}) starting {task}(*{args}) on worker {self.worker_id}')

        # Any task options used by the worker (here) should come from the registered task, not the message
        # this is to reduce message size, and also because publisher-worker is a shared python environment.
        # Meaning, the service, including some producers, may never see the @task() registration
        if message.get('bind') is True or dmethod.bind:
            args = [self.produce_binder(message)] + args

        try:
            return _call(*args, **kwargs)
        except DispatcherCancel:
            # Log exception because this can provide valuable info about where a task was when getting signal
            logger.exception(f'Worker {self.worker_id} task canceled (uuid={self.get_uuid(message)})')
            return '<cancel>'

    def perform_work(self, message):
        """
        Import and run code for a task e.g.,

        body = {
            'args': [8],
            'callbacks': [{
                'args': [],
                'kwargs': {}
                'task': u'awx.main.tasks.system.handle_work_success'
            }],
            'errbacks': [{
                'args': [],
                'kwargs': {},
                'task': 'awx.main.tasks.system.handle_work_error'
            }],
            'kwargs': {},
            'task': u'awx.main.tasks.jobs.RunProjectUpdate'
        }
        """
        # TODO: callback before starting task, previously ran
        # settings.__clean_on_fork__()
        result = None
        try:
            result = self.run_callable(message)
        except Exception as exc:
            result = exc

            try:
                if getattr(exc, 'is_awx_task_error', False):
                    # Error caused by user / tracked in job output
                    logger.warning("{}".format(exc))
                else:
                    task = message['task']
                    args = message.get('args', [])
                    kwargs = message.get('kwargs', {})
                    logger.exception('Worker failed to run task {}(*{}, **{}'.format(task, args, kwargs))
            except Exception:
                # It's fairly critical that this code _not_ raise exceptions on logging
                # If you configure external logging in a way that _it_ fails, there's
                # not a lot we can do here; sys.stderr.write is a final hail mary
                _, _, tb = sys.exc_info()
                traceback.print_tb(tb)

            for callback in message.get('errbacks', []) or []:
                callback['uuid'] = self.get_uuid(message)
                self.perform_work(callback)
        finally:
            # TODO: callback after running a task, previously ran
            # kube_config._cleanup_temp_files()
            pass

        for callback in message.get('callbacks', []) or []:
            callback['uuid'] = self.get_uuid(message)
            self.perform_work(callback)
        return result

    # NOTE: on_start and on_stop were intentionally removed
    # these were used for the consumer classes, but not the worker classes

    # TODO: new WorkerTaskCall class to track timings and such
    def get_finished_message(self, raw_result, message, time_started):
        """I finished the task in message, giving result. This is what I send back to traffic control."""
        result = None
        if type(raw_result) in (type(None), list, dict, int, str):
            result = raw_result
        elif isinstance(raw_result, Exception):
            pass  # already logged when task errors
        else:
            logger.info(f'Discarding task (uuid={self.get_uuid(message)}) result of non-serializable type {type(raw_result)}')

        return {
            "worker": self.worker_id,
            "event": "done",
            "result": result,
            "uuid": self.get_uuid(message),
            "time_started": time_started,
            "time_finish": time.time(),
        }

    def get_ready_message(self):
        """Message for traffic control, saying am entering the main work loop and am HOT TO GO"""
        return {"worker": self.worker_id, "event": "ready"}

    def get_shutdown_message(self):
        """Message for traffic control, do not deliver any more mail to this address"""
        return {"worker": self.worker_id, "event": "shutdown"}


def work_loop(worker_id: int, settings: dict, finished_queue: multiprocessing.Queue, message_queue: multiprocessing.Queue) -> None:
    """
    Worker function that processes messages from the queue and sends confirmation
    to the finished_queue once done.
    """
    # Load settings passed from parent
    # this assures that workers are all configured the same
    setup(config=settings)
    worker = TaskWorker(worker_id)
    # TODO: add an app callback here to set connection name and things like that

    finished_queue.put(worker.get_ready_message())

    while True:
        if worker.should_exit():
            break

        try:
            message = message_queue.get()
        except DispatcherCancel:
            logger.info(f'Worker {worker_id} received a task cancel signal in main loop, ignoring')
            continue
        except QueueEmpty:
            logger.info(f'Worker {worker_id} Encountered strange QueueEmpty condition')
            continue  # a race condition that mostly can be ignored
        except Exception as exc:
            logger.exception(f"Exception on worker {worker_id}, type {type(exc)}, error: {str(exc)}, exiting")
            break

        if not isinstance(message, dict):

            if isinstance(message, str):
                if message.lower() == "stop":
                    logger.warning(f"Worker {worker_id} exiting main loop due to stop message.")
                    break

            try:
                message = json.loads(message)
            except Exception as e:
                logger.error(f'Worker {worker_id} could not process message {message}, error: {str(e)}')
                break

        time_started = time.time()
        result = worker.perform_work(message)

        # Indicate that the task is finished by putting a message in the finished_queue
        finished_queue.put(worker.get_finished_message(result, message, time_started))

    finished_queue.put(worker.get_shutdown_message())
    logger.debug(f'Worker {worker_id} informed the pool manager that we have exited')
