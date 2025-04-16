import logging
import time
from typing import Any, Callable, Coroutine, Iterator, Optional, cast

from ..protocols import DelayCapsule as DelayCapsuleProtocol
from ..protocols import Delayer as DelayerProtocol
from ..protocols import SharedAsyncObjects as SharedAsyncObjectsProtocol
from .next_wakeup_runner import HasWakeup, NextWakeupRunner

logger = logging.getLogger(__name__)


class DelayCapsule(HasWakeup, DelayCapsuleProtocol):
    """When a task has a delay, this tracks the delay, as in a time capsule"""

    def __init__(self, delay: float, message: dict) -> None:
        self.has_ran: bool = False
        self.received_at = time.monotonic()
        self.delay = delay
        self.message = message

    def next_wakeup(self) -> Optional[float]:
        if self.has_ran is True:
            return None
        return self.received_at + self.delay


class Delayer(NextWakeupRunner, DelayerProtocol):
    def __init__(
        self,
        process_message_now: Callable[[dict[Any, Any]], Coroutine[Any, Any, tuple[str | None, str | None]]],
        shared: SharedAsyncObjectsProtocol,
    ) -> None:
        self.delayed_messages: set[DelayCapsuleProtocol] = set()
        self.process_message_now = process_message_now
        super().__init__(
            wakeup_objects=cast(set[HasWakeup], self.delayed_messages),
            process_object=self.run_delayed_capsule,  # type: ignore[arg-type] # takes capsules, not HasWakeups, which is more specific
            name='delayed_task_runner',
            shared=shared,
        )

    def __iter__(self) -> Iterator[DelayCapsuleProtocol]:
        return iter(self.delayed_messages)

    async def shutdown(self) -> None:
        await self.kick()
        for capsule in self.delayed_messages:
            logger.warning(f'Abandoning delayed task (due to shutdown) to run in {capsule.delay}, message={capsule.message}')
        self.delayed_messages = set()

    async def create_delayed_task(self, delay: float, message: dict) -> None:
        "Called as alternative to sending to worker now, send to worker later"
        capsule = DelayCapsule(delay, message)
        logger.info(f'Delaying {capsule.delay} s before running task: {capsule.message}')
        self.delayed_messages.add(capsule)
        await self.kick()

    def remove_capsule(self, capsule: DelayCapsuleProtocol) -> None:
        self.delayed_messages.remove(capsule)

    async def run_delayed_capsule(self, capsule: DelayCapsuleProtocol, /) -> None:
        """Mark the capsule as having been run for race conditions, remove it from list, call the actual task dispatching method"""
        capsule.has_ran = True
        logger.debug(f'Wakeup for delayed task: {capsule.message}')
        reply_to, payload = await self.process_message_now(capsule.message)
        if reply_to:
            logger.warning(f'Can not return reply to channel {reply_to} from delayed tasks, dropping reply:\n{payload}')
        self.remove_capsule(capsule)

    async def process_task(self, message: dict) -> Optional[dict]:
        """The general contract for process_task is that we can _consume_ the task and return None

        Here, task consumption means that we store it in the local storage to run later at delayed time.
        Otherwise, if this is not marked for delayed we hand the task back as return value.
        """
        if delay := message.pop('delay', None):
            # NOTE: control messages with reply should never be delayed, document this for users
            await self.create_delayed_task(delay, message)
            return None

        return message
