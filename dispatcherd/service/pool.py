import asyncio
import logging
import multiprocessing
import os
import queue
import signal
import time
from collections import OrderedDict
from typing import Any, Iterator, Literal

from ..processors.blocker import Blocker
from ..processors.queuer import Queuer
from ..protocols import DispatcherMain
from ..protocols import PoolEvents as PoolEventsProtocol
from ..protocols import PoolWorker as PoolWorkerProtocol
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from ..protocols import WorkerData as WorkerDataProtocol
from ..protocols import WorkerPool as WorkerPoolProtocol
from .asyncio_tasks import ensure_fatal
from .next_wakeup_runner import HasWakeup, NextWakeupRunner
from .process import ProcessManager, ProcessProxy

logger = logging.getLogger(__name__)


class PoolWorker(HasWakeup, PoolWorkerProtocol):
    def __init__(self, worker_id: int, process: ProcessProxy) -> None:
        self.worker_id = worker_id
        self.process = process
        self.current_task: dict | None = None
        self.created_at: float = time.monotonic()
        self.started_at: float | None = None
        self.stopping_at: float | None = None
        self.retired_at: float | None = None
        self.is_active_cancel: bool = False
        # Tracking information for worker
        self.finished_count = 0
        self.status: Literal['initialized', 'spawned', 'starting', 'ready', 'stopping', 'exited', 'error', 'retired'] = 'initialized'
        self.exit_msg_event = asyncio.Event()

    async def start(self) -> None:
        if self.status != 'initialized':
            logger.error(f'Worker {self.worker_id} status is not initialized, can not start, status={self.status}')
            return
        self.status = 'spawned'
        try:
            self.process.start()
        except Exception:
            logger.exception(f'Unexpected error starting worker {self.worker_id} subprocess, marking as error')
            self.status = 'error'
            return
        logger.debug(f'Worker {self.worker_id} pid={self.process.pid} subprocess has spawned')
        self.status = 'starting'  # Not ready until it sends callback message

    @property
    def is_ready(self) -> bool:
        """Worker is ready to receive task requests"""
        return bool(self.status == 'ready')

    @property
    def counts_for_capacity(self) -> bool:
        """Worker is ready to accept work or may become ready very soon, relevant for scale-up decisions"""
        return bool(self.status in ('initialized', 'spawned', 'starting', 'ready'))

    @property
    def expected_alive(self) -> bool:
        """Worker is expected to have an active process"""
        return bool(self.status in ('starting', 'ready'))

    @property
    def inactive(self) -> bool:
        """No further shutdown or callback messages are expected from this worker"""
        return bool(self.status in ('exited', 'error', 'initialized', 'retired'))

    async def start_task(self, message: dict) -> None:
        self.current_task = message  # NOTE: this marks this worker as busy
        self.process.message_queue.put(message)
        self.started_at = time.monotonic()

    async def join(self, timeout: int = 3) -> None:
        logger.debug(f'Joining worker {self.worker_id} pid={self.process.pid} subprocess')
        self.process.join(timeout)  # argument is timeout

    async def signal_stop(self, *, cancel_if_busy: bool = True) -> None:
        "Tell the worker to stop and return"
        self.process.message_queue.put("stop")
        logger.debug(f'Sent stop message to worker_id={self.worker_id}')
        if cancel_if_busy and self.current_task:
            uuid = self.current_task.get('uuid', '<unknown>')
            logger.warning(f'Worker {self.worker_id} is currently running task (uuid={uuid}), canceling for shutdown')
            self.cancel()
        self.status = 'stopping'
        self.stopping_at = time.monotonic()

    async def stop(self) -> None:
        "Tell the worker to stop, and do not return until the worker process is no longer running"
        if self.status in ('retired', 'error'):
            return  # already stopped

        if self.status not in ('stopping', 'exited'):
            await self.signal_stop()

        # Poll for process status with shorter intervals
        poll_interval = 0.2  # 200ms intervals
        total_timeout = 3.0  # 3 second total timeout
        start_time = time.monotonic()

        while time.monotonic() - start_time < total_timeout:
            if not self.process.is_alive():
                # Process already exited, likely due to process group SIGTERM
                if self.exit_msg_event.is_set():
                    logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited after signaling shutdown, skipping error transition')
                else:
                    logger.info(f'Worker {self.worker_id} pid={self.process.pid} already exited, likely due to process group signal')
                    self.status = 'error'  # Set status to 'error' instead of 'exited'
                    self.exit_msg_event.set()  # Set event to prevent other code from waiting
                    self.process.message_queue.close()
                break

            try:
                # Try to wait for exit message with a short timeout
                await asyncio.wait_for(self.exit_msg_event.wait(), timeout=poll_interval)
                break  # Exit message received
            except asyncio.TimeoutError:
                continue  # Keep polling

        else:  # Loop completed without break
            logger.error(f'Worker {self.worker_id} pid={self.process.pid} failed to send exit message in {total_timeout} seconds')
            self.status = 'error'
            self.process.message_queue.close()

        await self.join()  # If worker fails to exit, this returns control without raising an exception

        for i in range(3):
            if self.process.is_alive():
                logger.error(f'Worker {self.worker_id} pid={self.process.pid} is still alive trying SIGKILL, attempt {i}')
                await asyncio.sleep(1)
                self.process.kill()
            else:
                logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited code={self.process.exitcode()}')
                # Only set to 'retired' if exit code is 0 (clean exit), otherwise keep as 'error'
                if self.status != 'error' and self.process.exitcode() == 0:
                    self.status = 'retired'
                self.retired_at = time.monotonic()
                return

        logger.critical(f'Worker {self.worker_id} pid={self.process.pid} failed to exit after SIGKILL')
        self.status = 'error'
        self.retired_at = time.monotonic()
        return

    def cancel(self) -> None:
        self.is_active_cancel = True  # signal for result callback

        # If the process has never been started or is already gone, its pid may be None
        pid = self.process.pid
        if pid is None or not self.process.is_alive():
            logger.debug(f'Worker {self.worker_id} cancel skipped; process already exited')
            return  # it's effectively already canceled/not running
        try:
            os.kill(pid, signal.SIGUSR1)  # Use SIGUSR1 instead of SIGTERM
        except ProcessLookupError:
            logger.debug(f'Worker {self.worker_id} cancel skipped; pid {pid} missing')

    def get_status_data(self) -> dict[str, Any]:
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

    def next_wakeup(self) -> float | None:
        """Used by next-run-runner for setting wakeups for task timeouts"""
        if self.is_active_cancel:
            return None
        if self.current_task and self.current_task.get('timeout') and self.started_at:
            return self.started_at + self.current_task['timeout']
        return None


class PoolEvents(PoolEventsProtocol):
    "Benchmark tests have to re-create this because they use same object in different event loops"

    def __init__(self) -> None:
        self.queue_cleared: asyncio.Event = asyncio.Event()  # queue is now 0 length
        self.work_cleared: asyncio.Event = asyncio.Event()  # Totally quiet, no blocked or queued messages, no busy workers
        self.management_event: asyncio.Event = asyncio.Event()  # Process spawning is backgrounded, so this is the kicker
        self.workers_ready: asyncio.Event = asyncio.Event()  # min workers have started and sent ready message


class WorkerData(WorkerDataProtocol):
    def __init__(self) -> None:
        self.workers: OrderedDict[int, PoolWorker] = OrderedDict()
        self.management_lock = asyncio.Lock()

    def __iter__(self) -> Iterator[PoolWorker]:
        return iter(self.workers.values())

    def __contains__(self, worker_id: int) -> bool:
        return worker_id in self.workers

    def __len__(self) -> int:
        return len(self.workers)

    def add_worker(self, worker: PoolWorker) -> None:
        self.workers[worker.worker_id] = worker

    def get_by_id(self, worker_id: int) -> PoolWorker:
        return self.workers[worker_id]

    def remove_by_id(self, worker_id: int) -> None:
        del self.workers[worker_id]

    def move_to_end(self, worker_id: int) -> None:
        try:
            self.workers.move_to_end(worker_id)
        except KeyError:
            logger.warning(f'Attempted to move worker_id={worker_id} to end, but worker was already removed from workers dict')


class WorkerPool(WorkerPoolProtocol):
    def __init__(
        self,
        process_manager: ProcessManager,
        shared: SharedAsyncObjectsProtocol,
        min_workers: int = 1,
        max_workers: int | None = None,
        scaledown_wait: float = 15.0,
        scaledown_interval: float = 15.0,
        worker_stop_wait: float = 30.0,
        worker_removal_wait: float = 30.0,
        worker_max_lifetime_seconds: float | None = 4 * 60 * 60,
        results_read_timeout: float | None = None,
    ) -> None:
        self.min_workers = min_workers

        if max_workers is None:
            max_workers = multiprocessing.cpu_count()

        self.max_workers = max_workers
        self.process_manager = process_manager
        self.shared = shared

        # internal asyncio tasks
        self.read_results_task: asyncio.Task | None = None
        self.management_task: asyncio.Task | None = None
        # other internal asyncio objects
        self.events: PoolEventsProtocol = PoolEvents()

        # internal tracking variables
        self.workers = WorkerData()
        self.next_worker_id = 0
        self.finished_count: int = 0
        self.canceled_count: int = 0
        self.retirement_count: int = 0
        self.shutdown_timeout = 3

        # the timeout runner keeps its own task
        self.timeout_runner = NextWakeupRunner(self.workers, self.cancel_worker, shared=shared, name='worker_timeout_manager')

        # Track the last time we used X number of workers, like
        # {
        #   0: None,
        #   1: None,
        #   2: <timestamp>
        # }
        # where 1 worker is currently in use, and a task using the 2nd worker
        # finished at <timestamp>, which is compared against out scale down wait time
        self.last_used_by_ct: dict[int, float | None] = {}
        self.scaledown_wait = scaledown_wait
        self.scaledown_interval = scaledown_interval  # seconds for poll to see if we should retire workers
        self.worker_stop_wait = worker_stop_wait  # seconds to wait for a worker to exit on its own before SIGTERM, SIGKILL
        self.worker_removal_wait = worker_removal_wait  # after worker process exits, seconds to keep its record, for stats
        self.worker_max_lifetime_seconds = worker_max_lifetime_seconds
        self.results_read_timeout = results_read_timeout  # seconds to wait for finished_queue

        # queuer and blocker objects hold an internal inventory of tasks that can not yet run
        self.queuer = Queuer(self.workers)
        self.blocker = Blocker(self.queuer)

    async def retire_aged_workers(self) -> None:
        """Schedule retires for workers older than the configured lifetime"""
        if self.worker_max_lifetime_seconds is None:
            return

        max_lifetime = self.worker_max_lifetime_seconds
        now = time.monotonic()
        async with self.workers.management_lock:
            for worker in list(self.workers):
                if worker.status != 'ready':
                    continue
                worker_age = now - worker.created_at
                if worker_age < max_lifetime:
                    continue
                if worker.current_task:
                    uuid = worker.current_task.get('uuid', '<unknown>')
                    logger.info(f'Worker {worker.worker_id} reached max lifetime while running task (uuid={uuid}), sending stop request after completion')
                await worker.signal_stop(cancel_if_busy=False)
                self.retirement_count += 1

    @property
    def processed_count(self) -> int:
        return self.finished_count + self.canceled_count + self.blocker.discard_count

    @property
    def received_count(self) -> int:
        return self.processed_count + self.queuer.count() + self.blocker.count() + sum(1 for w in self.workers if w.current_task)

    def get_status_data(self) -> dict[str, Any]:
        return {
            "next_worker_id": self.next_worker_id,
            "finished_count": self.finished_count,
            "canceled_count": self.canceled_count,
            "retirement_count": self.retirement_count,
        }

    async def start_working(self, dispatcher: DispatcherMain) -> None:
        if self.process_manager.has_shutdown():
            logger.info("Process manager was shut down; recreating for a new run")
            self.process_manager = self.process_manager.recreate()
        # NOTE: any of these critical tasks throwing unexpected errors should halt program by setting exit_event
        self.read_results_task = ensure_fatal(
            asyncio.create_task(self.read_results_forever(dispatcher=dispatcher), name='results_task'), exit_event=self.shared.exit_event
        )
        self.management_task = ensure_fatal(asyncio.create_task(self.manage_workers(), name='management_task'), exit_event=self.shared.exit_event)

    def get_running_count(self) -> int:
        ct = 0
        for worker in self.workers:
            if worker.current_task:
                ct += 1
        return ct

    def should_scale_down(self) -> bool:
        "If True, we have not had enough work lately to justify the number of workers we are running"
        worker_ct = len([worker for worker in self.workers if worker.counts_for_capacity])
        last_used = self.last_used_by_ct.get(worker_ct)
        if last_used:
            delta = time.monotonic() - last_used
            # Criteria - last time we used this-many workers was greater than the setting
            return bool(delta > self.scaledown_wait)
        return False

    async def scale_workers(self) -> int:
        """Initiates scale-up and scale-down actions

        Note that, because we are very async, this may just set the action in motion.
        This does not fork a new process for scale-up, just creates a worker in memory.
        Instead of fully decomissioning a worker for scale-down, it just sends a stop message.
        Later on, we will reconcile data to get the full decomissioning outcome.
        """
        available_workers = [worker for worker in self.workers if worker.counts_for_capacity]
        worker_ct = len(available_workers)
        active_task_ct = self.active_task_ct()
        changed_ct = 0

        if worker_ct < self.min_workers:
            # Scale up to MIN for startup, or scale _back_ up to MIN if workers exited due to external signals
            worker_ids = []
            for _ in range(self.min_workers - worker_ct):
                new_worker_id = await self.up()
                worker_ids.append(new_worker_id)
                changed_ct += 1
            logger.info(f'Starting subprocess for workers ids={worker_ids} (prior ct={worker_ct}) to satisfy min_workers')

        elif active_task_ct > worker_ct:
            # have more messages to process than what we have workers
            if worker_ct < self.max_workers:
                # Scale up, below or to MAX
                new_worker_id = await self.up()
                changed_ct += 1
                logger.info(f'Started worker id={new_worker_id} (prior ct={worker_ct}) to handle queue pressure')
            else:
                # At MAX, nothing we can do, but let the user know anyway
                logger.warning(f'System at max_workers={self.max_workers} and queue pressure detected, capacity may be insufficient')

        elif active_task_ct == worker_ct:
            # Workers are exactly sufficient for queued work
            pass

        elif worker_ct > self.min_workers:
            # Scale down above or to MIN, because surplus of workers have done nothing useful in <cutoff> time
            async with self.workers.management_lock:
                if self.should_scale_down():
                    for worker in available_workers:
                        if worker.current_task is None:
                            logger.info(f'Scaling down worker id={worker.worker_id} (prior ct={worker_ct}) due to demand')
                            await worker.signal_stop()
                            changed_ct -= 1
                            break

        logger.debug(f'Ran scale_workers worker_ct={worker_ct}, active_task_ct={active_task_ct}, changed {changed_ct}')
        return changed_ct

    async def manage_new_workers(self) -> None:
        """This calls the .start() method to actually fork a new process for initialized workers

        This call may be slow. It is only called from the worker management task. This is its job.
        The forking_and_connecting_lock is shared with producers, and avoids forking and connecting at the same time.
        """
        for worker in self.workers:
            if worker.status == 'initialized':
                async with self.shared.forking_and_connecting_lock:  # never fork while connecting
                    await worker.start()
                # Starting the worker may have freed capacity for queued work
                await self.drain_queue()

    async def manage_old_workers(self) -> None:
        """Clear internal memory of workers whose process has exited, and assures processes are gone

        This method takes a snapshot of the current workers under lock,
        processes them outside the lock (including awaiting worker stops),
        and then re-acquires the lock to remove workers marked for deletion.

        happy path:
        The scale_workers method notifies a worker they need to exit
        The read_results_task will mark the worker status to exited
        This method will see the updated status, join the process, and remove it from self.workers
        """
        # Loop for process liveliness check
        current_workers = list(self.workers)
        for worker in current_workers:
            # Check if the worker has died unexpectedly.
            if worker.expected_alive and not worker.process.is_alive():
                async with self.workers.management_lock:
                    logger.error(f'Worker {worker.worker_id} pid={worker.process.pid} has died unexpectedly, status was {worker.status}')

                    if worker.current_task:
                        uuid = worker.current_task.get('uuid', '<unknown>')
                        logger.error(f'Task (uuid={uuid}) was running on worker {worker.worker_id} but the worker died unexpectedly')
                        self.canceled_count += 1
                        worker.is_active_cancel = False  # Prevent further processing.
                    worker.status = 'error'
                    worker.retired_at = time.monotonic()

        # Loop for worker accounting, for stopping workers or removing old workers from memory
        # Get a consistent snapshot of workers, also force stop of non-responding workers
        workers_to_stop: list[PoolWorker] = []
        async with self.workers.management_lock:
            current_workers = list(self.workers)

            # Send stop message to workers if necessary, or update worker status if totally done
            for worker in current_workers:

                if worker.status == 'exited':
                    workers_to_stop.append(worker)  # happy path
                elif (
                    worker.status == 'stopping'
                    and worker.stopping_at
                    and (time.monotonic() - worker.stopping_at) > self.worker_stop_wait
                    and (worker.is_active_cancel or worker.current_task is None)
                ):
                    logger.warning(f'Worker id={worker.worker_id} failed to respond to stop signal')
                    workers_to_stop.append(worker)  # agressively bring down process

        # Await worker stops outside the lock to avoid blocking the results task.
        for worker in workers_to_stop:
            await worker.stop()

        # Remove fully-done workers from memory
        async with self.workers.management_lock:
            remove_ids = []
            for worker in self.workers:
                if worker.status in ['retired', 'error'] and worker.retired_at and (time.monotonic() - worker.retired_at) > self.worker_removal_wait:
                    remove_ids.append(worker.worker_id)

            for worker_id in remove_ids:
                if worker_id in self.workers:
                    retired_at = self.workers.get_by_id(worker_id).retired_at
                    if retired_at:
                        delta = time.monotonic() - retired_at
                    else:
                        delta = 0
                    logger.debug(f'Fully removing worker id={worker_id}, retired {delta} seconds ago')
                    self.workers.remove_by_id(worker_id)

    async def manage_workers(self) -> None:
        """Enforces worker policy like min and max workers, and later, auto scale-down"""
        while not self.shared.exit_event.is_set():

            await self.retire_aged_workers()
            scaled_ct = await self.scale_workers()

            await self.manage_new_workers()

            # Do not look at old workers if we are scaling up
            if scaled_ct <= 0:
                await self.manage_old_workers()
            else:
                logger.debug(f'Not tending to old workers due to recent scaling up by {scaled_ct}')

            try:
                await asyncio.wait_for(self.events.management_event.wait(), timeout=self.scaledown_interval)
            except asyncio.TimeoutError:
                pass
            self.events.management_event.clear()

        logger.debug('Pool worker management task exiting')

    async def cancel_worker(self, worker: PoolWorker) -> None:
        """Writes a log and sends cancel signal to worker"""
        if (not worker.current_task) or (not worker.started_at):
            return  # mostly for typing, should not be the case
        # typing note - if we are here current_task is not None
        uuid: str = worker.current_task.get('uuid', '<unknown>')
        timeout = worker.current_task.get("timeout")
        logger.info(
            f'Worker {worker.worker_id} runtime {time.monotonic() - worker.started_at:.5f}(s) for task uuid={uuid} exceeded timeout {timeout}(s), canceling'
        )
        worker.cancel()

    async def up(self) -> int:
        new_worker_id = self.next_worker_id
        process = self.process_manager.create_process(kwargs={'worker_id': self.next_worker_id})
        worker = PoolWorker(new_worker_id, process)
        self.workers.add_worker(worker)
        self.next_worker_id += 1
        return new_worker_id

    async def stop_workers(self) -> None:
        # Stop any workers that are expected to be running
        workers_to_stop = []
        for worker in self.workers:
            async with self.workers.management_lock:
                if worker.counts_for_capacity:
                    await worker.signal_stop()
                    workers_to_stop.append(worker)
        stop_tasks = [worker.stop() for worker in workers_to_stop]
        await asyncio.gather(*stop_tasks)

        # Triple-check that all workers are stopped in practice
        for worker in self.workers:
            if worker.process.is_alive():
                logger.warning(f'worker_id={worker.worker_id} was found alive unexpectedly')
                await worker.stop()

    async def _shutdown_management_task(self) -> None:
        self.events.management_event.set()
        management_task = self.management_task
        if not management_task:
            return
        try:
            await asyncio.wait_for(management_task, timeout=self.shutdown_timeout)
        except asyncio.TimeoutError:
            logger.warning('Timed out waiting %.2fs for management task to finish, canceling', self.shutdown_timeout)
            management_task.cancel()
        except asyncio.CancelledError:
            logger.info('Worker management task canceled during shutdown')
        finally:
            self.management_task = None

    async def _shutdown_work_queues(self) -> None:
        await self.timeout_runner.kick()
        self.queuer.shutdown()
        self.blocker.shutdown()

    async def _stop_and_cleanup_workers(self) -> None:
        await self.stop_workers()
        for worker in self.workers:
            if worker.counts_for_capacity and worker.process.pid and worker.process.is_alive():
                logger.warning(f'Force killing worker {worker.worker_id} pid={worker.process.pid}')
                worker.process.kill()

    async def _shutdown_results_task(self) -> None:
        read_results_task = self.read_results_task
        if not read_results_task:
            return
        if read_results_task.done():
            logger.info('Finished watcher already completed, skipping shutdown logic')
            self.read_results_task = None
            return
        self.process_manager.send_finished_queue_stop(timeout=self.shutdown_timeout / 2.0)

        logger.info('Waiting for the finished watcher to return')
        try:
            await asyncio.wait_for(read_results_task, timeout=self.shutdown_timeout)
        except asyncio.TimeoutError:
            logger.warning('Timed out waiting %.2fs for finished watcher to exit, canceling', self.shutdown_timeout)
            read_results_task.cancel()
        except asyncio.CancelledError:
            logger.info('The finished task was canceled, but we are shutting down so that is alright')
        self.read_results_task = None

    async def shutdown(self) -> None:
        self.shared.exit_event.set()
        await self._shutdown_management_task()
        await self._shutdown_work_queues()
        await self._stop_and_cleanup_workers()
        await self._shutdown_results_task()
        self.process_manager.shutdown()

        logger.info('Pool is shut down')

    def active_task_ct(self) -> int:
        "The number of tasks currently being ran, or immediently eligible to run"
        return self.get_running_count() + self.queuer.count()

    async def post_task_start(self, message: dict) -> None:
        if 'timeout' in message:
            await self.timeout_runner.kick()  # kick timeout task to set wakeup
        running_ct = self.get_running_count()
        self.last_used_by_ct[running_ct] = None  # block scale down of this amount

    async def dispatch_task(self, message: dict) -> None:
        uuid = message.get("uuid", "<unknown>")
        worker = None
        if unblocked_task := await self.blocker.process_task(message):
            if self.shared.exit_event.is_set():
                logger.warning(f'Not dispatching task (uuid={uuid}) because currently shutting down')
                self.queuer.queued_messages.append(unblocked_task)
                return
            async with self.workers.management_lock:
                worker = self.queuer.get_worker_or_process_task(unblocked_task)
                if worker:
                    logger.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")
                    await worker.start_task(unblocked_task)  # transitions status, need under lock
            if worker:
                await self.post_task_start(unblocked_task)
            else:
                self.events.management_event.set()  # kick manager task to start auto-scale up if needed

    async def drain_queue(self) -> None:
        async with self.workers.management_lock:
            # First move all unblocked tasks into the blocked-on-capacity queue
            for message in self.blocker.pop_unblocked_messages():
                self.queuer.queued_messages.append(message)

        # Now process all messages we can in the unblocked queue
        processed_queue = False
        for message in self.queuer.queued_messages.copy():
            if (not self.queuer.get_free_worker()) or self.shared.exit_event.is_set():
                return
            self.queuer.queued_messages.remove(message)
            await self.dispatch_task(message)
            processed_queue = True

        if processed_queue:
            self.events.queue_cleared.set()

    async def process_finished(self, worker: PoolWorker, message: dict) -> None:
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

        running_ct = self.get_running_count()
        self.last_used_by_ct[running_ct] = time.monotonic()  # scale down may be allowed, clock starting now

        is_stopping = message.get('is_stopping', False)

        # Mark the worker as no longer busy
        async with self.workers.management_lock:
            if worker.is_active_cancel and result == '<cancel>':
                self.canceled_count += 1
            else:
                self.finished_count += 1
            if is_stopping:
                worker.status = 'stopping'
                if not worker.stopping_at:
                    worker.stopping_at = time.monotonic()
            worker.mark_finished_task()
            self.workers.move_to_end(worker.worker_id)

        if not self.queuer.queued_messages and all(worker.current_task is None for worker in self.workers):
            self.events.work_cleared.set()

        if 'timeout' in message:
            await self.timeout_runner.kick()

    @property
    def status_counts(self) -> dict[str, int]:
        """Debugging data for logs only

        example format:
        {'retired': 8, 'exited': 1, 'stopping': 3}
        """
        stats: dict[str, int] = {}
        for worker in self.workers:
            stats.setdefault(worker.status, 0)
            stats[worker.status] += 1
        return stats

    async def read_results_forever(self, dispatcher: DispatcherMain) -> None:
        """Perpetual task that continuously waits for task completions."""
        while True:
            try:
                message = await self.process_manager.read_finished(timeout=self.results_read_timeout)
            except queue.Empty:
                if self.shared.exit_event.is_set():
                    logger.warning('Finished queue read timed out during shutdown, exiting results task')
                    return
                continue
            except (EOFError, OSError, RuntimeError, TypeError):
                if self.shared.exit_event.is_set() or self.process_manager.has_shutdown():
                    logger.warning('Finished queue read failed during shutdown, exiting results task')
                    return
                raise

            if message == 'stop':
                logger.debug(f'Results message got administrative stop message, worker status: {self.status_counts}')
                return

            if not isinstance(message, dict):
                logger.error("Results message missing data: expected dict, got %s", type(message).__name__)
                continue

            if "worker" not in message or "event" not in message:
                logger.error("Results message missing keys: %s", message)
                continue

            worker_id = int(message["worker"])
            event = message["event"]
            worker = self.workers.get_by_id(worker_id)

            if event == 'ready':
                worker.status = 'ready'
                if all(worker.status == 'ready' for worker in self.workers):
                    self.events.workers_ready.set()
                await self.drain_queue()

            elif event == 'shutdown':
                async with self.workers.management_lock:
                    worker.status = 'exited'
                    worker.exit_msg_event.set()
                    worker.process.message_queue.close()

                if self.shared.exit_event.is_set():
                    if all(worker.inactive for worker in self.workers):
                        logger.debug(f"Worker {worker_id} exited and that is all of them, exiting results read task.")
                        return
                    else:
                        logger.debug(
                            f"Worker {worker_id} exited and that is a good thing because we are trying to shut down. Remaining statuses: {self.status_counts}"
                        )
                else:
                    self.events.management_event.set()
                    logger.debug(f"Worker {worker_id} sent exit signal.")

            elif event == 'control':
                action = message.get('command', 'unknown')
                try:
                    return_data = await dispatcher.get_control_result(  # type: ignore[union-attr]
                        str(action), control_data=message.get('control_data', {})  # type: ignore[var-annotated,arg-type]
                    )
                except Exception:
                    logger.exception('Error with control request from worker task')
                    return_data = {'error': 'Failed to run control task'}

                worker.process.message_queue.put(return_data)

            elif event == 'done':
                await self.process_finished(worker, message)
                await self.drain_queue()
