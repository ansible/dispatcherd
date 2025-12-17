import asyncio
import inspect
import time

import pytest

from dispatcherd.publish import task


@task(queue="test_channel")
def sigterm_interceptor_task():
    """
    This function is top-level in an importable module.
    The dispatcher sees e.g. "tests.integration.test_signal_handling.sigterm_interceptor_task".
    """
    import signal
    import sys

    def on_sigterm(signum, frame):
        sys.stdout.write("SIGTERM was intercepted!\n")
        sys.stdout.flush()

    signal.signal(signal.SIGTERM, on_sigterm)
    while True:
        time.sleep(0.1)


@pytest.mark.asyncio
async def test_sigusr1_cancel_avoids_sigterm(apg_dispatcher, pg_control, test_settings, caplog):
    """
    Ensures dispatcher uses SIGUSR1, not SIGTERM. This test demonstrates canceling a task via SIGUSR1 without triggering
    a custom SIGTERM handler.
    """

    # Submit the task
    uuid_val = "test-sigusr1-cancel-inline"
    sigterm_interceptor_task.apply_async(uuid=uuid_val, settings=test_settings)

    # Give the dispatcher a moment to start the task
    await asyncio.sleep(0.3)

    # Cancel the running task
    # canceled_jobs e.g. [ { "node_id": ..., "worker-0": {...} } ]
    canceled_jobs = await pg_control.acontrol_with_reply("cancel", data={"uuid": uuid_val}, timeout=2)
    assert canceled_jobs, "Expected at least some data from the cancel reply."
    canceled_info = canceled_jobs[0]

    # Wait for the dispatcher to finish cleaning up the task
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.wait_for(clearing_task, timeout=5)

    # We want to confirm at least one "worker-X" sub-dict has the correct UUID
    node_id = canceled_info.pop("node_id", None)
    assert node_id, "No node_id in reply"
    assert canceled_info, f"Missing worker data: {canceled_info}"

    # We'll search for any subdict that has 'uuid': our expected value
    (worker_key, worker_data) = list(canceled_info.items())[0]
    assert worker_data["uuid"] == uuid_val

    # Check that 1 task was canceled, and no SIGTERM printout
    assert apg_dispatcher.pool.canceled_count == 1
    assert "SIGTERM was intercepted!" not in caplog.text
