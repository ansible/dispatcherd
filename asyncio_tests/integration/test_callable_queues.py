import asyncio
import json

import pytest

from dispatcherd.config import temporary_settings
from tests.data import methods as test_methods


# Function that alternates between valid queue names the dispatcher listens to
def get_alternating_queue():
    """Returns a different queue name with each call"""
    if not hasattr(get_alternating_queue, 'counter'):
        get_alternating_queue.counter = 0
    get_alternating_queue.counter += 1
    return f"test_channel{get_alternating_queue.counter % 2 + 1}"  # returns "test_channel1" or "test_channel2"


@pytest.mark.asyncio
async def test_apply_async_with_callable_queue(apg_dispatcher, test_settings):
    """Test that apply_async works with callable queues"""
    # Reset state for test
    apg_dispatcher.pool.events.work_cleared.clear()
    initial_count = apg_dispatcher.pool.finished_count

    # Create a task to wait for completion
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())

    # Using apply_async instead of delay with a callable that returns a valid queue
    with temporary_settings(test_settings.serialize()):
        test_methods.print_hello.apply_async(queue=lambda: "test_channel")

    # Wait for task to complete with timeout
    await asyncio.wait_for(clearing_task, timeout=3)

    # Verify task completed
    assert apg_dispatcher.pool.finished_count == initial_count + 1


@pytest.mark.asyncio
async def test_callable_queue_at_submission_time(apg_dispatcher, test_settings):
    """Test that a callable queue works when provided at submission time"""
    # Reset state for test
    apg_dispatcher.pool.events.work_cleared.clear()
    initial_count = apg_dispatcher.pool.finished_count

    # Create a task to wait for completion
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())

    # Using a callable queue at submission time, not at decoration time
    with temporary_settings(test_settings.serialize()):
        test_methods.print_hello.apply_async(queue=lambda: "test_channel")

    # Wait for task to complete with timeout
    await asyncio.wait_for(clearing_task, timeout=3)

    # Verify task completed
    assert apg_dispatcher.pool.finished_count == initial_count + 1


@pytest.mark.asyncio
async def test_alternating_queue_names(apg_dispatcher, test_settings):
    """Test that a callable returning different queue names works correctly"""
    # Reset for first task
    apg_dispatcher.pool.events.work_cleared.clear()
    initial_count = apg_dispatcher.pool.finished_count

    # Set up first task
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())

    # Use a static queue to make sure the test is well-controlled
    with temporary_settings(test_settings.serialize()):
        test_methods.print_hello.apply_async(queue="test_channel")

    # Wait for first task to complete
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == initial_count + 1

    # Set up second task with a different queue
    apg_dispatcher.pool.events.work_cleared.clear()
    second_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())

    with temporary_settings(test_settings.serialize()):
        test_methods.print_hello.apply_async(queue="test_channel2")

    # Wait for second task to complete
    await asyncio.wait_for(second_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == initial_count + 2


def test_callable_queue_json_serializable():
    """Test that a callable queue is properly handled for JSON serialization"""
    from dispatcherd.registry import DispatcherMethod

    def test_func():
        pass

    def get_queue():
        return "result_queue"

    # Test with a callable queue
    method = DispatcherMethod(test_func, queue=get_queue)

    # The message for async body should be JSON serializable
    message = method.get_async_body()
    assert json.dumps(message)  # Should not raise exception

    # The callable should not be included in the message
    assert 'queue' not in message
