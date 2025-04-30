import logging
import random
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional, Literal

from ..processors.params import ProcessorParams
from ..protocols import PoolWorker
from ..protocols import Queuer as QueuerProtocol

logger = logging.getLogger(__name__)


class Queuer(QueuerProtocol):
    @dataclass(kw_only=True)
    class Params(ProcessorParams):
        delay: float = 0.0

    def __init__(self, workers: Iterable[PoolWorker], worker_selection_strategy: Literal['linear', 'random', 'longest-idle'] = 'linear') -> None:
        self.queued_messages: list[dict] = []  # TODO: use deque, customizability
        self.workers = workers
        self.worker_selection_strategy = worker_selection_strategy

    def __iter__(self) -> Iterator[dict]:
        return iter(self.queued_messages)

    def count(self) -> int:
        return len(self.queued_messages)

    def get_free_worker(self) -> Optional[PoolWorker]:
        free_workers = [w for w in self.workers if (not w.current_task) and w.is_ready]
        if not free_workers:
            return None

        if self.worker_selection_strategy == 'random':
            return random.choice(free_workers)
        elif self.worker_selection_strategy == 'longest-idle':
            # Sort by last_task_finished_at, with None (never had a task) being considered oldest
            return min(free_workers, key=lambda w: w.last_task_finished_at if w.last_task_finished_at is not None else float('-inf'))
        else:  # linear strategy
            return free_workers[0]

    def active_tasks(self) -> Iterator[dict]:
        """Iterable of all tasks currently running, or eligable to be ran right away"""
        for task in self.queued_messages:
            yield task

        for worker in self.workers:
            if worker.current_task:
                yield worker.current_task

    def remove_task(self, message: dict) -> None:
        self.queued_messages.remove(message)

    def get_worker_or_process_task(self, message: dict) -> Optional[PoolWorker]:
        """Either give a worker to place the task on, or put message into queue

        In the future we may change to optionally discard some tasks.
        """
        uuid = message.get("uuid", "<unknown>")
        if worker := self.get_free_worker():
            logger.debug(f"Dispatching task (uuid={uuid}) to worker (id={worker.worker_id})")
            return worker
        else:
            logger.warning(f'Queueing task (uuid={uuid}), due to lack of capacity, queued_ct={len(self.queued_messages)}')
            self.queued_messages.append(message)
            return None

    def shutdown(self) -> None:
        """Just write log messages about what backed up work we will lose"""
        if self.queued_messages:
            uuids = [message.get('uuid', '<unknown>') for message in self.queued_messages]
            logger.error(f'Dispatcherd shut down with queued work, uuids: {uuids}')
