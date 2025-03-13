import asyncio
import logging
import os
import signal
import time
from typing import Any, Literal, Optional

from .asyncio_tasks import ensure_fatal
from .blocker import Blocker
from .next_wakeup_runner import HasWakeup, NextWakeupRunner
from .process import ProcessManager, ProcessProxy
from .queuer import Queuer
from .scaler import Scaler, ScalerConfig

logger = logging.getLogger(__name__)


class PoolWorker(HasWakeup):
    def __init__(self, worker_id: int, process: ProcessProxy) -> None:
        self.worker_id = worker_id
        self.process = process
        self.current_task: Optional[dict] = None
        self.created_at: float = time.monotonic()
        self.started_at: Optional[float] = None
        self.stopping_at: Optional[float] = None
        self.retired_at: Optional[float] = None
        self.is_active_cancel: bool = False

        # Tracking information for worker
        self.finished_count = 0
        self.status: Literal['initialized', 'spawned', 'starting', 'ready', 'stopping', 'exited', 'error', 'retired'] = 'initialized'
        self.exit_msg_event = asyncio.Event()

    def is_ready(self):
        """Worker is ready to receive task requests"""
        return bool(self.status == 'ready')

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
    def counts_for_capacity(self) -> bool:
        return bool(self.status in ('initialized', 'spawned', 'starting', 'ready'))

    async def start_task(self, message: dict) -> None:
        self.current_task = message  # NOTE: this marks this worker as busy
        self.process.message_queue.put(message)
        self.started_at = time.monotonic()

    async def join(self, timeout=3) -> None:
        logger.debug(f'Joining worker {self.worker_id} pid={self.process.pid} subprocess')
        self.process.join(timeout)  # argument is timeout

    async def signal_stop(self) -> None:
        "Tell the worker to stop and return"
        self.process.message_queue.put("stop")
        if self.current_task:
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

        try:
            if self.status != 'exited':
                await asyncio.wait_for(self.exit_msg_event.wait(), timeout=3)
            self.exit_msg_event.clear()
        except asyncio.TimeoutError:
            logger.error(f'Worker {self.worker_id} pid={self.process.pid} failed to send exit message in 3 seconds')
            self.status = 'error'  # can signal for result task to exit, since no longer waiting for it here

        await self.join()  # If worker fails to exit, this returns control without raising an exception

        for i in range(3):
            if self.process.is_alive():
                logger.error(f'Worker {self.worker_id} pid={self.process.pid} is still alive trying SIGKILL, attempt {i}')
                await asyncio.sleep(1)
                self.process.kill()
            else:
                logger.debug(f'Worker {self.worker_id} pid={self.process.pid} exited code={self.process.exitcode()}')
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

    def next_wakeup(self) -> Optional[float]:
        """Used by next-run-runner for setting wakeups for task timeouts"""
        if self.is_active_cancel:
            return None
        if self.current_task and self.current_task.get('timeout') and self.started_at:
            return self.started_at + self.current_task['timeout']
        return None


class PoolEvents:
    "Benchmark tests have to re-create this because they use same object in different event loops"

    def __init__(self) -> None:
        self.queue_cleared: asyncio.Event = asyncio.Event()  # queue is now 0 length
        self.work_cleared: asyncio.Event = asyncio.Event()  # Totally quiet, no blocked or queued messages, no busy workers
        self.management_event: asyncio.Event = asyncio.Event()  # Process spawning is backgrounded, so this is the kicker
        self.workers_ready: asyncio.Event = asyncio.Event()  # min workers have started and sent ready message


class WorkerPool:
    def __init__(self, process_manager: ProcessManager, scaler_config: ScalerConfig) -> None:
        self.process_manager = process_manager

        # internal asyncio tasks
        self.read_results_task: Optional[asyncio.Task] = None
        # other internal asyncio objects
        self.management_lock = asyncio.Lock()
        self.events: PoolEvents = PoolEvents()

        # internal tracking variables
        self.workers: dict[int, PoolWorker] = {}
        self.next_worker_id = 0
        self.shutting_down = False
        self.finished_count: int = 0
        self.canceled_count: int = 0
        self.shutdown_timeout = 3

        # the timeout runner keeps its own task
        self.timeout_runner = NextWakeupRunner(self.workers.values(), self.cancel_worker, name='worker_timeout_manager')

        self.scaler = Scaler(workers=self.workers, management_lock=self.management_lock, config=scaler_config)

        # queuer and blocker objects hold an internal inventory of tasks that can not yet run
        self.queuer = Queuer(self.workers.values())
        self.blocker = Blocker(self.queuer)

    @property
    def processed_count(self):
        return self.finished_count + self.canceled_count + self.blocker.discard_count

    @property
    def received_count(self):
        return self.processed_count + self.queuer.count() + self.blocker.count() + sum(1 for w in self.workers.values() if w.current_task)

    async def start_working(self, forking_lock: asyncio.Lock, exit_event: Optional[asyncio.Event] = None) -> None:
        self.read_results_task = ensure_fatal(asyncio.create_task(self.read_results_forever(), name='results_task'), exit_event=exit_event)
        self.scaler.exit_event = exit_event
        await self.scaler.kick()
        self.timeout_runner.exit_event = exit_event

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
        self.workers[new_worker_id] = worker
        self.next_worker_id += 1
        return new_worker_id

    async def stop_workers(self) -> None:
        for worker in self.workers.values():
            await worker.signal_stop()
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
        await self.timeout_runner.shutdown()
        self.queuer.shutdown()
        self.blocker.shutdown()
        await self.stop_workers()
        self.process_manager.finished_queue.put('stop')
        await self.scaler.shutdown()

        if self.read_results_task:
            logger.info('Waiting for the finished watcher to return')
            try:
                await asyncio.wait_for(self.read_results_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning(f'The finished task failed to cancel in {self.shutdown_timeout} seconds, will force.')
                await self.force_shutdown()
            except asyncio.CancelledError:
                logger.info('The finished task was canceled, but we are shutting down so that is alright')

        if self.management_task:
            logger.info('Canceling worker management task')
            self.management_task.cancel()
            try:
                await asyncio.wait_for(self.management_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.error('The scaleup task failed to shut down')
            except asyncio.CancelledError:
                pass  # intended

        logger.info('Pool is shut down')

    async def post_task_start(self, message: dict) -> None:
        if 'timeout' in message:
            await self.timeout_runner.kick()  # kick timeout task to set wakeup
        self.scaler.task_started_update()

    async def dispatch_task(self, message: dict) -> None:
        uuid = message.get("uuid", "<unknown>")
        async with self.management_lock:
            if unblocked_task := self.blocker.process_task(message):
                if worker := self.queuer.get_worker_or_process_task(unblocked_task):
                    logger.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")
                    await worker.start_task(unblocked_task)
                    await self.post_task_start(unblocked_task)
                else:
                    self.events.management_event.set()  # kick manager task to start auto-scale up

    async def drain_queue(self) -> None:
        async with self.management_lock:
            # First move all unblocked tasks into the blocked-on-capacity queue
            for message in self.blocker.pop_unblocked_messages():
                self.queuer.queued_messages.append(message)

        # Now process all messages we can in the unblocked queue
        processed_queue = False
        for message in self.queuer.queued_messages.copy():
            if (not self.queuer.get_free_worker()) or self.shutting_down:
                return
            self.queuer.queued_messages.remove(message)
            await self.dispatch_task(message)
            processed_queue = True

        if processed_queue:
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

        self.scaler.task_finished_update()

        # Mark the worker as no longer busy
        async with self.management_lock:
            if worker.is_active_cancel and result == '<cancel>':
                self.canceled_count += 1
            else:
                self.finished_count += 1
            worker.mark_finished_task()

        if not self.queuer.queued_messages and all(worker.current_task is None for worker in self.workers.values()):
            self.events.work_cleared.set()

        if 'timeout' in message:
            await self.timeout_runner.kick()

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
                    self.events.management_event.set()
                    logger.debug(f"Worker {worker_id} sent exit signal.")

            elif event == 'done':
                await self.process_finished(worker, message)
                await self.drain_queue()
