import logging
import multiprocessing
import os
import signal
import sys
import time
import traceback
from typing import Any, Optional, Union

from ..protocols import TaskWorker as TaskWorkerProtocol
from ..registry import DispatcherMethodRegistry
from ..registry import registry as global_registry
from .exceptions import DispatcherCancel

logger = logging.getLogger(__name__)


class WorkerSignalHandler:
    def __init__(self, worker_id: int) -> None:
        self.kill_now = False
        self.worker_id = worker_id
        signal.signal(signal.SIGUSR1, self.task_cancel)
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def task_cancel(self, *args, **kwargs) -> None:  # type:ignore[no-untyped-def]
        raise DispatcherCancel

    def exit_gracefully(self, *args, **kwargs) -> None:  # type:ignore[no-untyped-def]
        logger.info(f'Worker {self.worker_id} received worker process exit signal')
        self.kill_now = True


class DispatcherBoundMethods:
    """
    If you use the task decorator with the bind=True argument,
    an object of this type will be passed in.
    This contains public methods for users of the dispatcher to call.
    """

    def __init__(self, worker_id: int, message: dict, message_queue: multiprocessing.Queue, finished_queue: multiprocessing.Queue) -> None:
        self.worker_id = worker_id
        self.message_queue = message_queue
        self.finished_queue = finished_queue
        self.uuid = message.get('uuid', '<unknown>')

    def control(self, command: str, data: Optional[dict] = None) -> dict:
        to_send = {'worker': self.worker_id, 'event': 'control', 'command': command}
        if data:
            to_send['control_data'] = data
        self.finished_queue.put(to_send)
        return self.message_queue.get()


class TaskWorker(TaskWorkerProtocol):
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

    def __init__(
        self,
        worker_id: int,
        message_queue: multiprocessing.Queue,
        finished_queue: multiprocessing.Queue,
        registry: DispatcherMethodRegistry = global_registry,
        idle_timeout: int = 0,
    ) -> None:
        self.worker_id: int = worker_id
        self.message_queue = message_queue
        self.finished_queue = finished_queue
        self.registry = registry
        self.ppid = os.getppid()
        self.pid = os.getpid()
        self.signal_handler = WorkerSignalHandler(worker_id)
        self.idle_timeout = idle_timeout

    def on_start(self) -> None:
        """For apps integrating callbacks"""
        pass

    def on_shutdown(self) -> None:
        """For apps integrating callbacks"""
        pass

    def pre_task(self, message: dict) -> None:
        """For apps integrating callbacks"""
        pass

    def post_task(self, result: dict) -> None:
        """The result is a part of the contract if someone adds callbacks"""
        pass

    def on_idle(self) -> None:
        """For apps integrating callbacks"""
        pass

    def should_exit(self) -> bool:
        """Called before continuing the loop, something suspicious, return True, should exit"""
        if os.getppid() != self.ppid:
            logger.error(f'Worker {self.worker_id}, my parent PID changed, this process has been orphaned, like segfault or sigkill, exiting')
            return True
        elif self.signal_handler.kill_now:
            return True
        return False

    def get_uuid(self, message: dict[str, Any]) -> str:
        return message.get('uuid', '<unknown>')

    def produce_binder(self, message: dict) -> DispatcherBoundMethods:
        """
        Return the object with public callbacks to pass to the task
        """
        return DispatcherBoundMethods(worker_id=self.worker_id, message=message, message_queue=self.message_queue, finished_queue=self.finished_queue)

    def run_callable(self, message: dict) -> Any:
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

    def perform_work(self, message: dict) -> dict[str, Any]:
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
        time_started = time.time()
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
        return self.get_finished_message(result, message, time_started)

    # TODO: new WorkerTaskCall class to track timings and such
    def get_finished_message(self, raw_result: Any, message: dict, time_started: float) -> dict[str, Any]:
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

    def get_ready_message(self) -> dict[str, Union[str, int]]:
        """Message for traffic control, saying am entering the main work loop"""
        return {"worker": self.worker_id, "event": "ready"}

    def get_shutdown_message(self) -> dict[str, Union[str, int]]:
        """Message for traffic control, do not deliver any more mail to this address"""
        return {"worker": self.worker_id, "event": "shutdown"}
