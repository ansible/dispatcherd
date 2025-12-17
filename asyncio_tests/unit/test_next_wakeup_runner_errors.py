import time

import pytest

from dispatcherd.service.next_wakeup_runner import HasWakeup, NextWakeupRunner
from dispatcherd.service.asyncio_tasks import SharedAsyncObjects


# Dummy object that implements HasWakeup.
class DummySchedule(HasWakeup):
    def __init__(self, wakeup_time: float):
        self._wakeup_time = wakeup_time

    def next_wakeup(self) -> float:
        return self._wakeup_time


# Dummy process_object that simulates successful processing by pushing wakeup time forward.
async def dummy_process_object(schedule: DummySchedule) -> None:
    # Simulate processing by adding 10 seconds.
    schedule._wakeup_time += 10


# Dummy process_object that raises an exception.
async def failing_process_object(schedule: DummySchedule) -> None:
    raise ValueError("Processing error")


@pytest.mark.asyncio
async def test_process_wakeups_normal():
    # Set up a dummy schedule with a wakeup time in the past.
    past_time = time.monotonic() - 5
    schedule = DummySchedule(past_time)
    # Use dummy_process_object that adds 10 seconds.
    runner = NextWakeupRunner([schedule], dummy_process_object, shared=SharedAsyncObjects())
    current_time = time.monotonic()
    next_wakeup = await runner.process_wakeups(current_time)
    # The wakeup time should now be 10 seconds later than the original past time.
    assert next_wakeup == schedule._wakeup_time
    # Also, since the schedule was processed, it should not return None.
    assert next_wakeup is not None


@pytest.mark.asyncio
async def test_process_wakeups_error_propagation():
    # Set up a dummy schedule with a wakeup time in the past.
    past_time = time.monotonic() - 5
    schedule = DummySchedule(past_time)
    # Use failing_process_object that raises an exception.
    runner = NextWakeupRunner([schedule], failing_process_object, shared=SharedAsyncObjects())
    current_time = time.monotonic()
    with pytest.raises(ValueError, match="Processing error"):
        await runner.process_wakeups(current_time)
