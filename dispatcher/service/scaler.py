import asyncio
import logging
import time
from typing import Literal, Optional, Protocol

from .asyncio_tasks import ensure_fatal

logger = logging.getLogger(__name__)


class ScalerConfig:
    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 4,
        scaledown_wait: float = 15.0,
        scaledown_interval: float = 15.0,
        worker_stop_wait: float = 30.0,
        worker_removal_wait: float = 30.0,
    ) -> None:
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scaledown_wait = scaledown_wait
        self.scaledown_interval = scaledown_interval  # seconds for poll to see if we should retire workers
        self.worker_stop_wait = worker_stop_wait  # seconds to wait for a worker to exit on its own before SIGTERM, SIGKILL
        self.worker_removal_wait = worker_removal_wait  # after worker process exits, seconds to keep its record, for stats


class PoolWorkerProto(Protocol):
    status: Literal['initialized', 'spawned', 'starting', 'ready', 'stopping', 'exited', 'error', 'retired'] = 'initialized'
    worker_id: int
    current_task: Optional[dict]
    retired_at: float

    @property
    def counts_for_capacity(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def signal_stop(self) -> None: ...


class Scaler:
    def __init__(self, workers: dict[int, PoolWorkerProto], management_lock: asyncio.Lock, config: ScalerConfig) -> None:
        self.workers = workers
        self.management_lock = management_lock
        self.config = config
        # Track the last time we used X number of workers, like
        # {
        #   0: None,
        #   1: None,
        #   2: <timestamp>
        # }
        # where 1 worker is currently in use, and a task using the 2nd worker
        # finished at <timestamp>, which is compared against out scale down wait time
        self.last_used_by_ct: dict[int, Optional[float]] = {}

        self.management_task: Optional[asyncio.Task] = None
        self.management_event = asyncio.Event()
        self.shutting_down: bool = False

    def should_scale_down(self) -> bool:
        "If True, we have not had enough work lately to justify the number of workers we are running"
        worker_ct = len([worker for worker in self.workers.values() if worker.counts_for_capacity])
        last_used = self.last_used_by_ct.get(worker_ct)
        if last_used:
            delta = time.monotonic() - last_used
            # Criteria - last time we used this-many workers was greater than the setting
            return bool(delta > self.config.scaledown_wait)
        return False

    async def scale_workers(self) -> None:
        """Initiates scale-up and scale-down actions

        Note that, because we are very async, this may just set the action in motion.
        This does not fork a new process for scale-up, just creates a worker in memory.
        Instead of fully decomissioning a worker for scale-down, it just sends a stop message.
        Later on, we will reconcile data to get the full decomissioning outcome.
        """
        available_workers = [worker for worker in self.workers.values() if worker.counts_for_capacity]
        worker_ct = len(available_workers)

        if worker_ct < self.config.min_workers:
            # Scale up to MIN for startup, or scale _back_ up to MIN if workers exited due to external signals
            worker_ids = []
            for _ in range(self.config.min_workers - worker_ct):
                new_worker_id = await self.up()
                worker_ids.append(new_worker_id)
            logger.info(f'Starting subprocess for workers ids={worker_ids} (prior ct={worker_ct}) to satisfy min_workers')

        elif self.active_task_ct() > len(available_workers):
            # have more messages to process than what we have workers
            if worker_ct < self.config.max_workers:
                # Scale up, below or to MAX
                new_worker_id = await self.up()
                logger.info(f'Started worker id={new_worker_id} (prior ct={worker_ct}) to handle queue pressure')
            else:
                # At MAX, nothing we can do, but let the user know anyway
                logger.warning(f'System at max_workers={self.config.max_workers} and queue pressure detected, capacity may be insufficient')

        elif worker_ct > self.config.min_workers:
            # Scale down above or to MIN, because surplus of workers have done nothing useful in <cutoff> time
            async with self.management_lock:
                if self.should_scale_down():
                    for worker in available_workers:
                        if worker.current_task is None:
                            logger.info(f'Scaling down worker id={worker.worker_id} (prior ct={worker_ct}) due to demand')
                            await worker.signal_stop()
                            break

    async def manage_new_workers(self, forking_lock: asyncio.Lock) -> None:
        """This calls the .start() method to actually fork a new process for initialized workers

        This call may be slow. It is only called from the worker management task. This is its job.
        The forking_lock is shared with producers, and avoids forking and connecting at the same time.
        """
        for worker in self.workers.values():
            if worker.status == 'initialized':
                async with forking_lock:  # never fork while connecting
                    await worker.start()
                    # Leaves worker in starting state, WorkerPool will get ready message
                    # when ready message comes in, it will dispatch new tasks, so do not do it here

    async def manage_old_workers(self) -> None:
        """Clear internal memory of workers whose process has exited, and assures processes are gone

        happy path:
        The scale_workers method notifies a worker they need to exit
        The read_results_task will mark the worker status to exited
        This method will see the updated status, join the process, and remove it from self.workers
        """
        remove_ids = []
        for worker in self.workers.values():
            # Check for workers that died unexpectedly
            if worker.status not in ['retired', 'error', 'exited', 'initialized', 'spawned'] and not worker.process.is_alive():
                logger.error(f'Worker {worker.worker_id} pid={worker.process.pid} has died unexpectedly, status was {worker.status}')

                if worker.current_task:
                    uuid = worker.current_task.get('uuid', '<unknown>')
                    logger.error(f'Task (uuid={uuid}) was running on worker {worker.worker_id} but the worker died unexpectedly')
                    self.canceled_count += 1
                    worker.is_active_cancel = False  # Ensure it's not processed by timeout runner

                worker.status = 'error'
                worker.retired_at = time.monotonic()

            if worker.status == 'exited':
                await worker.stop()  # happy path
            elif worker.status == 'stopping' and worker.stopping_at and (time.monotonic() - worker.stopping_at) > self.config.worker_stop_wait:
                logger.warning(f'Worker id={worker.worker_id} failed to respond to stop signal')
                await worker.stop()  # agressively bring down process

            elif worker.status in ['retired', 'error'] and worker.retired_at and (time.monotonic() - worker.retired_at) > self.config.worker_removal_wait:
                remove_ids.append(worker.worker_id)

        # Remove workers from memory, done as separate loop due to locking concerns
        for worker_id in remove_ids:
            async with self.management_lock:
                if worker_id in self.workers:
                    logger.debug(f'Fully removing worker id={worker_id}')
                    del self.workers[worker_id]

    async def manage_workers(self, forking_lock: asyncio.Lock) -> None:
        """Enforces worker policy like min and max workers, and later, auto scale-down"""
        while not self.shutting_down:

            await self.scale_workers()

            await self.manage_new_workers(forking_lock)

            await self.manage_old_workers()

            try:
                await asyncio.wait_for(self.management_event.wait(), timeout=self.config.scaledown_interval)
            except asyncio.TimeoutError:
                pass
            self.management_event.clear()

        logger.debug('Pool worker management task exiting')

    def unblocked_message_ct(self) -> int:
        "The number of queued tasks currently eligible to run"
        # TODO: somehow get the queuer here, then this will be easier
        unblocked_msg_ct = 0
        for message in self.queued_messages:
            if not self.message_is_blocked(message):
                unblocked_msg_ct += 1
        return unblocked_msg_ct

    def get_running_count(self) -> int:
        ct = 0
        for worker in self.workers.values():
            if worker.current_task:
                ct += 1
        return ct

    def active_task_ct(self) -> int:
        "The number of tasks currently being ran, or immediently eligible to run"
        return self.get_running_count() + self.queuer.count()

    def task_started_update(self):
        running_ct = self.get_running_count()
        self.last_used_by_ct[running_ct] = None  # block scale down of this amount

    def task_finished_update(self):
        running_ct = self.get_running_count()
        self.last_used_by_ct[running_ct] = time.monotonic()  # scale down may be allowed, clock starting now

    def mk_new_task(self) -> None:
        """Should only be called if a task is not currently running"""
        self.asyncio_task = asyncio.create_task(self.manage_workers(), name='worker_scaling_task')
        ensure_fatal(self.asyncio_task)

    async def kick(self) -> None:
        """Initiates the asyncio task to manage worker scaling, should only be needed at start"""
        if self.management_task:
            if self.management_task.done():
                self.mk_new_task()
            else:
                self.management_event.set()
        else:
            self.mk_new_task()

    async def shutdown(self):
        self.shutting_down = True
        self.management_event.set()
        if self.management_task:
            logger.info('Canceling worker management task')
            self.management_task.cancel()
            try:
                await asyncio.wait_for(self.management_task, timeout=self.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.error('The scaleup task failed to shut down')
            except asyncio.CancelledError:
                pass  # intended
