import logging
from dataclasses import dataclass
from typing import Iterator

from ..processors.params import ProcessorParams
from ..protocols import Queuer as QueuerProtocol
from ..protocols import WorkerData

logger = logging.getLogger(__name__)


QUEUE_LVL_MAP = {0: logging.DEBUG, 1: logging.INFO}


class Queuer(QueuerProtocol):
    @dataclass(kw_only=True)
    class Params(ProcessorParams):
        delay: float = 0.0

    def __init__(self, workers: WorkerData) -> None:
        self.queued_messages: list[dict] = []  # TODO: use deque, customizability
        self.workers = workers

    def __iter__(self) -> Iterator[dict]:
        return iter(self.queued_messages)

    def count(self) -> int:
        return len(self.queued_messages)

    async def active_tasks(self) -> list[dict]:
        """Snapshot of queued work plus tasks currently running."""
        tasks = list(self.queued_messages)

        worker_snapshot = await self.workers.snapshot()

        for worker in worker_snapshot:
            async with worker.lock:
                if worker.current_task:
                    tasks.append(worker.current_task)

        return tasks

    def remove_task(self, message: dict) -> None:
        self.queued_messages.remove(message)

    def queue_task(self, message: dict) -> None:
        """Put message into queue when capacity is exhausted."""
        uuid = message.get("uuid", "<unknown>")
        queue_ct = len(self.queued_messages)
        log_msg = f'Queueing task (uuid={uuid}), due to lack of capacity, queued_ct={queue_ct}'
        logging.log(QUEUE_LVL_MAP.get(queue_ct, logging.WARNING), log_msg)
        self.queued_messages.append(message)

    def shutdown(self) -> None:
        """Just write log messages about what backed up work we will lose"""
        if self.queued_messages:
            uuids = [message.get('uuid', '<unknown>') for message in self.queued_messages]
            logger.error(f'Dispatcherd shut down with queued work, uuids: {uuids}')
