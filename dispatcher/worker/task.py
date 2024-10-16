import inspect
import json
import logging
import os
import signal
import sys
import time
import traceback
from queue import Empty as QueueEmpty

from dispatcher.utils import resolve_callable

logger = logging.getLogger(__name__)


"""This module contains code ran by the worker subprocess"""


class WorkerSignalHandler:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def exit_gracefully(self, *args, **kwargs):
        logger.info('Received worker process exit signal')
        self.kill_now = True


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

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.ppid = os.getppid()
        self.pid = os.getpid()
        self.signal_handler = WorkerSignalHandler()

    def should_exit(self) -> str:
        """Called before continuing the loop, something suspicious, return True, should exit"""
        if os.getppid() != self.ppid:
            logger.error('My parent PID changed, this process has been orphaned, like segfault or sigkill, exiting')
            return True
        elif self.signal_handler.kill_now:
            logger.error('Exiting main loop of worker process due to interupt signal')
            return True
        return False

    def get_uuid(self, message):
        return message.get('uuid', '<unknown>')

    def run_callable(self, message):
        """
        Given some AMQP message, import the correct Python code and run it.
        """
        task = message['task']
        args = message.get('args', [])
        kwargs = message.get('kwargs', {})
        _call = resolve_callable(task)
        if inspect.isclass(_call):
            # the callable is a class, e.g., RunJob; instantiate and
            # return its `run()` method
            _call = _call().run

        # don't print kwargs, they often contain launch-time secrets
        logger.debug(f'task {self.get_uuid(message)} starting {task}(*{args}) on worker {self.worker_id}')

        return _call(*args, **kwargs)

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
    def get_finished_message(self, result, message, time_started):
        """I finished the task in message, giving result. This is what I send back to traffic control."""
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


def work_loop(worker_id, queue, finished_queue):
    """
    Worker function that processes messages from the queue and sends confirmation
    to the finished_queue once done.
    """
    worker = TaskWorker(worker_id)
    # TODO: add an app callback here to set connection name and things like that

    finished_queue.put(worker.get_ready_message())

    while True:
        if worker.should_exit():
            break

        try:
            message = queue.get()
        except QueueEmpty:
            logger.info(f'Worker {worker_id} Encountered strange QueueEmpty condition')
            continue  # a race condition that mostly can be ignored
        except Exception as exc:
            logger.exception(f"Exception on worker {worker_id}, type {type(exc)}, error: {str(exc)}, exiting")
            break

        if not isinstance(message, dict):

            if isinstance(message, str):
                if message.lower() == "stop":
                    logger.warning(f"Worker {worker_id} stopping.")
                    break

            try:
                message = json.loads(message)
            except Exception as e:
                logger.error(f'Worker {worker.worker_id} could not process message {message}, error: {str(e)}')
                break

        logger.info(f'message to perform_work on {message}')
        logger.info(f'the type {type(message)}')
        time_started = time.time()
        result = worker.perform_work(message)

        # Indicate that the task is finished by putting a message in the finished_queue
        finished_queue.put(worker.get_finished_message(result, message, time_started))

    finished_queue.put(worker.get_shutdown_message())
    logger.debug('Informed the pool manager that we have exited')
