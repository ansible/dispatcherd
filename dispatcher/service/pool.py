import asyncio
import logging
import os
import signal
import time
from asyncio import Task
from typing import Any, Iterator, Optional

from ..utils import DuplicateBehavior, MessageAction
from .process import ProcessManager, ProcessProxy

logger = logging.getLogger(__name__)


class PoolWorker:
    def __init__(self, worker_id: int, process: ProcessProxy) -> None:
        self.worker_id = worker_id
        self.process = process
        self.current_task: Optional[dict] = None
        self.created_at: float = time.monotonic()
        self.started_at: Optional[int] = None
        self.is_active_cancel: bool = False

        # Tracking information for worker
        self.finished_count = 0
        self.status = 'initialized'
        self.exit_msg_event = asyncio.Event()

    async def start(self) -> None:
        self.status = 'spawned'
        self.process.start()
        logger.debug(f'Worker {self.worker_id} pid={self.process.pid} subprocess has spawned')
        self.status = 'starting'  # Not ready until it sends callback message

    async def start_task(self, message: dict) -> None:
        self.current_task = message  # NOTE: this marks this worker as busy
        self.process.message_queue.put(message)
        self.started_at = time.monotonic_ns()

    async def join(self) -> None:
        logger.debug(f'Joining worker {self.worker_id} pid={self.process.pid} subprocess')
        self.process.join()

    async def stop(self) -> None:
        self.process.message_queue.put("stop")
        if self.current_task:
            uuid = self.current_task.get('uuid', '<unknown>')
            logger.warning(f'Worker {self.worker_id} is currently running task (uuid={uuid}), canceling for shutdown')
            self.cancel()

        try:
            await asyncio.wait_for(self.exit_msg_event.wait(), timeout=3)
            self.exit_msg_event.clear()
            self.process.join(3)  # argument is timeout
        except asyncio.TimeoutError:
            logger.error(f'Worker {self.worker_id} pid={self.process.pid} failed to send exit message in 3 seconds')
            self.status = 'error'  # can signal for result task to exit, since no longer waiting for it here

        for i in range(3):
            if self.process.is_alive():
                logger.error(f'Worker {self.worker_id} pid={self.process.pid} is still trying SIGKILL, attempt {i}')
                await asyncio.sleep(1)
                self.process.kill()
            else:
                logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited code={self.process.exitcode}')
                self.status = 'initialized'
                return

        logger.critical(f'Worker {self.worker_id} pid={self.process.pid} failed to exit after SIGKILL')
        self.status = 'error'
        return

    def cancel(self) -> None:
        self.is_active_cancel = True  # signal for result callback

        # If the process has never been started or is already gone, its pid may be None
        if self.process.pid is None:
            return  # it's effectively already canceled/not running
        os.kill(self.process.pid, signal.SIGUSR1)  # Use SIGUSR1 instead of SIGTERM

    def get_data(self) -> dict[str, Any]:
        return {
            'worker_id': self.worker_id,
            'pid': self.process.pid,
            'status': self.status,
            'finished_count': self.finished_count,
            'current_task': self.current_task.get('task') if self.current_task else None,
            'current_task_uuid': self.current_task.get('uuid', '<unknown>') if self.current_task else None,
            'active_cancel': self.is_active_cancel,
            'age': time.monotonic() - self.created_at,
        }

    def mark_finished_task(self) -> None:
        self.is_active_cancel = False
        self.current_task = None
        self.started_at = None
        self.finished_count += 1

    @property
    def inactive(self) -> bool:
        "Return True if no further shutdown or callback messages are expected from this worker"
        return self.status in ['exited', 'error', 'initialized']


class PoolEvents:
    "Benchmark tests have to re-create this because they use same object in different event loops"

    def __init__(self) -> None:
        self.queue_cleared: asyncio.Event = asyncio.Event()  # queue is now 0 length
        self.work_cleared: asyncio.Event = asyncio.Event()  # Totally quiet, no blocked or queued messages, no busy workers
        self.management_event: asyncio.Event = asyncio.Event()  # Process spawning is backgrounded, so this is the kicker
        self.timeout_event: asyncio.Event = asyncio.Event()  # Anything that might affect the timeout watcher task
        self.workers_ready: asyncio.Event = asyncio.Event()  # min workers have started and sent ready message


class WorkerPool:
    def __init__(self, max_workers: int, process_manager: ProcessManager):
        self.max_workers = max_workers
        self.workers: dict[int, PoolWorker] = {}
        self.next_worker_id = 0
        self.process_manager = process_manager
        self.queued_messages: list[dict] = []  # TODO: use deque, invent new kinds of logging anxiety
        self.read_results_task: Optional[Task] = None
        self.start_worker_task: Optional[Task] = None
        self.shutting_down = False
        self.finished_count: int = 0
        self.canceled_count: int = 0
        self.discard_count: int = 0
        self.shutdown_timeout = 3
        self.management_lock = asyncio.Lock()

        self.events: PoolEvents = PoolEvents()

    @property
    def processed_count(self):
        return self.finished_count + self.canceled_count + self.discard_count

    @property
    def received_count(self):
        return self.processed_count + len(self.queued_messages) + sum(1 for w in self.workers.values() if w.current_task)

    async def start_working(self, dispatcher) -> None:
        self.read_results_task = asyncio.create_task(self.read_results_forever(), name='results_task')
        self.read_results_task.add_done_callback(dispatcher.fatal_error_callback)
        self.management_task = asyncio.create_task(self.manage_workers(forking_lock=dispatcher.fd_lock), name='management_task')
        self.management_task.add_done_callback(dispatcher.fatal_error_callback)
        self.timeout_task = asyncio.create_task(self.manage_timeout(), name='timeout_task')
        self.timeout_task.add_done_callback(dispatcher.fatal_error_callback)

    async def manage_workers(self, forking_lock: asyncio.Lock) -> None:
        """Enforces worker policy like min and max workers, and later, auto scale-down"""
        while not self.shutting_down:
            while len(self.workers) < self.max_workers:
                await self.up()

            # TODO: if all workers are busy, queue has unblocked work, below max_workers
            # scale up 1 more worker in that case

            for worker in self.workers.values():
                if worker.status == 'initialized':
                    logger.debug(f'Starting subprocess for worker {worker.worker_id}')
                    async with forking_lock:  # never fork while connecting
                        await worker.start()

            await self.events.management_event.wait()
            self.events.management_event.clear()
        logger.debug('Pool worker management task exiting')

    async def process_worker_timeouts(self, current_time: float) -> Optional[int]:
        """
        Cancels tasks that have exceeded their timeout.
        Returns the system clock time of the next task timeout, for rescheduling.
        """
        next_deadline = None
        for worker in self.workers.values():
            if (not worker.is_active_cancel) and worker.current_task and worker.started_at and (worker.current_task.get('timeout')):
                timeout: float = worker.current_task['timeout']
                worker_deadline = worker.started_at + int(timeout * 1.0e9)

                # Established that worker is running a task that has a timeout
                if worker_deadline < current_time:
                    uuid: str = worker.current_task.get('uuid', '<unknown>')
                    delta: float = (current_time - worker.started_at) * 1.0e9
                    logger.info(f'Worker {worker.worker_id} runtime {delta:.5f}(s) for task uuid={uuid} exceeded timeout {timeout}(s), canceling')
                    worker.cancel()
                elif next_deadline is None or worker_deadline < next_deadline:
                    # worker timeout is closer than any yet seen
                    next_deadline = worker_deadline

        return next_deadline

    async def manage_timeout(self) -> None:
        while not self.shutting_down:
            current_time = time.monotonic_ns()
            pool_deadline = await self.process_worker_timeouts(current_time)
            if pool_deadline:
                time_until_deadline = (pool_deadline - current_time) * 1.0e-9
                try:
                    await asyncio.wait_for(self.events.timeout_event.wait(), timeout=time_until_deadline)
                except asyncio.TimeoutError:
                    pass  # will handle in next loop run
            else:
                await self.events.timeout_event.wait()
            self.events.timeout_event.clear()

    async def up(self) -> None:
        process = self.process_manager.create_process(kwargs={'worker_id': self.next_worker_id})
        worker = PoolWorker(self.next_worker_id, process)
        self.workers[self.next_worker_id] = worker
        self.next_worker_id += 1

    async def stop_workers(self) -> None:
        stop_tasks = [worker.stop() for worker in self.workers.values()]
        await asyncio.gather(*stop_tasks)

    async def force_shutdown(self) -> None:
        for worker in self.workers.values():
            if worker.process.pid and worker.process.is_alive():
                logger.warning(f'Force killing worker {worker.worker_id} pid={worker.process.pid}')
                worker.process.kill()

        if self.read_results_task:
            self.read_results_task.cancel()
            logger.info('Finished watcher had to be canceled, awaiting it a second time')
            try:
                await self.read_results_task
            except asyncio.CancelledError:
                pass

    async def shutdown(self) -> None:
        self.shutting_down = True
        self.events.management_event.set()
        self.events.timeout_event.set()
        await self.stop_workers()
        self.process_manager.finished_queue.put('stop')

        if self.read_results_task:
            logger.info('Waiting for the finished watcher to return')
            try:
                await asyncio.wait_for(self.read_results_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning(f'The finished task failed to cancel in {self.shutdown_timeout} seconds, will force.')
                await self.force_shutdown()
            except asyncio.CancelledError:
                logger.info('The finished task was canceled, but we are shutting down so that is alright')
            except Exception:
                # traceback logged in fatal callback
                if not hasattr(self.read_results_task, '_dispatcher_tb_logged'):
                    logger.exception('Pool shutdown saw an unexpected exception from results task')

        if self.start_worker_task:
            logger.info('Canceling worker spawn task')
            self.start_worker_task.cancel()
            try:
                await asyncio.wait_for(self.start_worker_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.error('The scaleup task failed to shut down')
            except asyncio.CancelledError:
                pass  # intended

        if self.queued_messages:
            uuids = [message.get('uuid', '<unknown>') for message in self.queued_messages]
            logger.error(f'Dispatcher shut down with queued work, uuids: {uuids}')

        logger.info('Pool is shut down')

    def get_free_worker(self) -> Optional[PoolWorker]:
        for candidate_worker in self.workers.values():
            if (not candidate_worker.current_task) and candidate_worker.status == 'ready':
                return candidate_worker
        return None

    def running_tasks(self) -> Iterator[dict]:
        for worker in self.workers.values():
            if worker.current_task:
                yield worker.current_task

    def _duplicate_in_list(self, message, task_iter) -> bool:
        for other_message in task_iter:
            if other_message is message:
                continue
            keys = ('task', 'args', 'kwargs')
            if all(other_message.get(key) == message.get(key) for key in keys):
                return True
        return False

    def already_running(self, message) -> bool:
        return self._duplicate_in_list(message, self.running_tasks())

    def already_queued(self, message) -> bool:
        return self._duplicate_in_list(message, self.queued_messages)

    def get_blocking_action(self, message: dict) -> str:
        on_duplicate = message.get('on_duplicate', DuplicateBehavior.parallel.value)

        if on_duplicate == DuplicateBehavior.serial.value:
            if self.already_running(message):
                return MessageAction.queue.value

        elif on_duplicate == DuplicateBehavior.discard.value:
            if self.already_running(message) or self.already_queued(message):
                return MessageAction.discard.value

        elif on_duplicate == DuplicateBehavior.queue_one.value:
            if self.already_queued(message):
                return MessageAction.discard.value
            elif self.already_running(message):
                return MessageAction.queue.value

        elif on_duplicate != DuplicateBehavior.parallel.value:
            logger.warning(f'Got unexpected on_duplicate value {on_duplicate}')

        return MessageAction.run.value

    def message_is_blocked(self, message: dict) -> bool:
        return bool(self.get_blocking_action(message) == MessageAction.queue.value)

    def get_unblocked_message(self) -> Optional[dict]:
        """Returns a message from the queue that is unblocked to run, if one exists"""
        for message in self.queued_messages:
            if not self.message_is_blocked(message):
                return message
        return None

    async def dispatch_task(self, message: dict) -> None:
        async with self.management_lock:
            uuid = message.get("uuid", "<unknown>")

            blocking_action = self.get_blocking_action(message)
            if blocking_action == MessageAction.discard.value:
                logger.info(f'Discarding task because it is already running: \n{message}')
                self.discard_count += 1
                return
            elif self.shutting_down:
                logger.info(f'Not starting task (uuid={uuid}) because we are shutting down, queued_ct={len(self.queued_messages)}')
                self.queued_messages.append(message)
                return
            elif blocking_action == MessageAction.queue.value:
                logger.info(f'Queuing task (uuid={uuid}) because it is already running or queued, queued_ct={len(self.queued_messages)}')
                self.queued_messages.append(message)
                return

            if worker := self.get_free_worker():
                logger.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")
                await worker.start_task(message)
                if 'timeout' in message:
                    self.events.timeout_event.set()  # kick timeout task to set wakeup
            else:
                logger.warning(f'Queueing task (uuid={uuid}), ran out of workers, queued_ct={len(self.queued_messages)}')
                self.queued_messages.append(message)
                self.events.management_event.set()  # kick manager task to start auto-scale up

    async def drain_queue(self) -> None:
        work_done = False
        while requeue_message := self.get_unblocked_message():
            if (not self.get_free_worker()) or self.shutting_down:
                return
            self.queued_messages.remove(requeue_message)
            await self.dispatch_task(requeue_message)
            work_done = True

        if work_done:
            self.events.queue_cleared.set()

    async def process_finished(self, worker, message) -> None:
        uuid = message.get('uuid', '<unknown>')
        msg = f"Worker {worker.worker_id} finished task (uuid={uuid}), ct={worker.finished_count}"
        result = None
        if message.get("result"):
            result = message["result"]
            if worker.is_active_cancel:
                msg += ', expected cancel'
            if result == '<cancel>':
                msg += ', canceled'
            else:
                msg += f", result: {result}"
        logger.debug(msg)

        # Mark the worker as no longer busy
        async with self.management_lock:
            if worker.is_active_cancel and result == '<cancel>':
                self.canceled_count += 1
            else:
                self.finished_count += 1
            worker.mark_finished_task()

        if not self.queued_messages and all(worker.current_task is None for worker in self.workers.values()):
            self.events.work_cleared.set()

        if 'timeout' in message:
            self.events.timeout_event.set()

    async def read_results_forever(self) -> None:
        """Perpetual task that continuously waits for task completions."""
        while True:
            # Wait for a result from the finished queue
            message = await self.process_manager.read_finished()

            if message == 'stop':
                if self.shutting_down:
                    stats = [worker.status for worker in self.workers.values()]
                    logger.debug(f'Results message got administrative stop message, worker status: {stats}')
                    return
                else:
                    logger.error('Results queue got stop message even through not shutting down')
                    continue

            worker_id = int(message["worker"])
            event = message["event"]
            worker = self.workers[worker_id]

            if event == 'ready':
                worker.status = 'ready'
                if all(worker.status == 'ready' for worker in self.workers.values()):
                    self.events.workers_ready.set()
                await self.drain_queue()

            elif event == 'shutdown':
                async with self.management_lock:
                    worker.status = 'exited'
                    worker.exit_msg_event.set()
                if self.shutting_down:
                    if all(worker.inactive for worker in self.workers.values()):
                        logger.debug(f"Worker {worker_id} exited and that is all of them, exiting results read task.")
                        return
                    else:
                        stats = [worker.status for worker in self.workers.values()]
                        logger.debug(f"Worker {worker_id} exited and that is a good thing because we are trying to shut down. Remaining: {stats}")
                else:
                    await worker.join()
                    async with self.management_lock:
                        del self.workers[worker.worker_id]
                        self.events.management_event.set()
                    logger.debug(f"Worker {worker_id} finished exiting. It will be restarted.")

            elif event == 'done':
                await self.process_finished(worker, message)
                await self.drain_queue()
