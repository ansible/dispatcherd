import asyncio
import time
from typing import Any, Dict

import pytest

from dispatcherd.service import DispatcherService
from dispatcherd.testing import adispatcher_service


async def sleep_task(seconds: float) -> Dict[str, Any]:
    await asyncio.sleep(seconds)
    return {"result": f"slept for {seconds} seconds"}


@pytest.mark.asyncio
async def test_worker_selection_by_idle_time() -> None:
    """Test that tasks are assigned to the worker that has been idle the longest."""
    settings = {
        "service": {
            "pool_kwargs": {
                "min_workers": 3,
                "max_workers": 3,
                "worker_stop_wait": 1.0,
                "worker_removal_wait": 1.0,
            }
        }
    }

    async with adispatcher_service(settings=settings) as service:
        dispatcher: DispatcherService = service.dispatcher

        # Submit tasks that will finish at different times
        # Worker 0: 2 second sleep
        # Worker 1: 1 second sleep
        # Worker 2: 3 second sleep
        task0 = await dispatcher.process_task({"task": "sleep_task", "args": [2.0]})
        task1 = await dispatcher.process_task({"task": "sleep_task", "args": [1.0]})
        task2 = await dispatcher.process_task({"task": "sleep_task", "args": [3.0]})

        # Wait for the 1-second task to finish
        await asyncio.sleep(1.1)
        
        # Submit a new task - this should go to Worker 1 since it finished first
        task3 = await dispatcher.process_task({"task": "sleep_task", "args": [0.1]})

        # Wait for all tasks to complete
        results = await asyncio.gather(
            task0,
            task1,
            task2,
            task3,
            return_exceptions=True,
        )

        # Verify the results
        assert all(not isinstance(r, Exception) for r in results)
        assert results[0]["result"] == "slept for 2.0 seconds"
        assert results[1]["result"] == "slept for 1.0 seconds"
        assert results[2]["result"] == "slept for 3.0 seconds"
        assert results[3]["result"] == "slept for 0.1 seconds"

        # Get the worker status to verify which worker handled task3
        worker_status = dispatcher.pool.get_status_data()
        # The worker that handled task3 should be the one that finished task1
        # We can verify this by checking the worker's last_task_finished_at time
        workers = list(dispatcher.pool.workers)
        worker_times = [(w.worker_id, w.last_task_finished_at) for w in workers if w.last_task_finished_at is not None]
        worker_times.sort(key=lambda x: x[1])  # Sort by finish time
        earliest_finish_worker = worker_times[0][0]  # Worker that finished first
        
        # The worker that handled task3 should be the one that finished earliest
        assert any(
            w.worker_id == earliest_finish_worker and w.current_task is not None
            for w in workers
        ), "Task3 was not assigned to the worker that had been idle the longest" 