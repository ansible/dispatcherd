import logging
from typing import Iterable, Iterator, Optional

from ..protocols import Blocker as BlockerProtocol
from ..utils import DuplicateBehavior
from .queuer import Queuer

logger = logging.getLogger(__name__)


class Blocker(BlockerProtocol):
    def __init__(self, queuer: Queuer) -> None:
        self.blocked_messages: list[dict] = []  # TODO: use deque, customizability
        self.queuer = queuer
        self.discard_count: int = 0

    def __iter__(self) -> Iterator[dict]:
        return iter(self.blocked_messages)

    def _duplicate_in_list(self, message: dict, task_iter: Iterable[dict]) -> bool:
        for other_message in task_iter:
            if other_message is message:
                continue
            keys = ('task', 'args', 'kwargs')
            if all(other_message.get(key) == message.get(key) for key in keys):
                return True
        return False

    def already_running(self, message: dict) -> bool:
        return self._duplicate_in_list(message, self.queuer.active_tasks())

    def already_queued(self, message: dict) -> bool:
        return self._duplicate_in_list(message, self.blocked_messages)

    def remove_task(self, message: dict) -> None:
        self.blocked_messages.remove(message)

    async def process_task(self, message: dict) -> Optional[dict]:
        """If task is blocked, it is consumed here and None is returned, if not blocked, return message as-is

        Consuming the message may mean discarding it, or it may mean holding it until it is unblocked.
        If task if not blocked and is returned, that means you should continue doing what you were going to do.
        """
        uuid = message.get("uuid", "<unknown>")
        on_duplicate = message.get('on_duplicate', DuplicateBehavior.parallel.value)

        if on_duplicate == DuplicateBehavior.serial.value:
            if self.already_running(message):
                logger.info(f'Queuing task (uuid={uuid}) because it is already running, queued_ct={len(self.blocked_messages)}')
                self.blocked_messages.append(message)
                return None

        elif on_duplicate == DuplicateBehavior.discard.value:
            if self.already_running(message) or self.already_queued(message):
                logger.info(f'Discarding task because it is already running: \n{message}')
                self.discard_count += 1
                return None

        elif on_duplicate == DuplicateBehavior.queue_one.value:
            if self.already_queued(message):
                logger.info(f'Discarding task because it is already running and queued: \n{message}')
                self.discard_count += 1
                return None
            elif self.already_running(message):
                logger.info(f'Queuing task (uuid={uuid}) because it is already running, queued_ct={len(self.blocked_messages)}')
                self.blocked_messages.append(message)
                return None

        elif on_duplicate != DuplicateBehavior.parallel.value:
            logger.warning(f'Got unexpected on_duplicate value {on_duplicate} in message {message}')

        return message

    def pop_unblocked_messages(self) -> list[dict]:
        now_unblocked: list[dict] = []
        for message in self.blocked_messages.copy():
            # All forms of blocking require task to not be running or queued or in the newly-released list in order to release
            if not (self.already_running(message) or self._duplicate_in_list(message, now_unblocked)):
                now_unblocked.append(message)
                self.blocked_messages.remove(message)
        return now_unblocked

    def count(self) -> int:
        return len(self.blocked_messages)

    def shutdown(self) -> None:
        """Issue a log about the tasks that will be abandoned due to the shutdown"""
        if self.blocked_messages:
            uuids = [message.get('uuid', '<unknown>') for message in self.blocked_messages]
            logger.error(f'Dispatcherd shut down with blocked work, uuids: {uuids}')
