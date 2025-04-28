import logging
from typing import Iterable, Iterator, Optional

from ..protocols import PoolWorker
from ..protocols import Queuer as QueuerProtocol

logger = logging.getLogger(__name__)


class Queuer(QueuerProtocol):
    def __init__(self, workers: Iterable[PoolWorker]) -> None:
        self.queued_messages: list[dict] = []  # TODO: use deque, customizability
        self.workers = workers

    def __iter__(self) -> Iterator[dict]:
        return iter(self.queued_messages)

    def count(self) -> int:
        return len(self.queued_messages)

    def get_free_worker(self) -> Optional[PoolWorker]:
        for candidate_worker in self.workers:
            if (not candidate_worker.current_task) and candidate_worker.is_ready:
                return candidate_worker
        return None

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
