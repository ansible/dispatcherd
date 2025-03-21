import asyncio
import json
import time
from typing import Union

import pytest

from dispatcherd.config import temporary_settings
from tests.data import methods as test_methods

SLEEP_METHOD = 'lambda: __import__("time").sleep(0.1)'


async def wait_to_receive(dispatcher, ct, timeout=5.0, interval=0.05):
    """Poll for the dispatcher to have received a certain ct of messages"""
    start = time.time()
    while time.time() - start < timeout:
        if dispatcher.pool.received_count >= ct:
            break
        await asyncio.sleep(interval)
    else:
        raise RuntimeError(f'Failed to receive expected {ct} messages {dispatcher.pool.received_count}')


@pytest.mark.asyncio
async def test_run_lambda_function(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait(), name='test_lambda_clear_wait')
    await pg_message('lambda: "This worked!"')
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_run_decorated_function(apg_dispatcher, test_settings):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    test_methods.print_hello.apply_async(settings=test_settings)
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == 1

    # piggyback test for method with callable queue, not static
    apg_dispatcher.pool.events.work_cleared.clear()
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with temporary_settings(test_settings.serialize()):
        test_methods.use_callable_queue.delay()
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == 2

    # piggyback again to test class method with callable queue
    apg_dispatcher.pool.events.work_cleared.clear()
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with temporary_settings(test_settings.serialize()):
        test_methods.RunJob.delay()
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == 3


@pytest.mark.asyncio
async def test_submit_with_global_settings(apg_dispatcher, test_settings):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with temporary_settings(test_settings.serialize()):
        test_methods.print_hello.delay()  # settings are inferred from global context
    await asyncio.wait_for(clearing_task, timeout=3)
    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_multiple_channels(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.gather(
        pg_message(SLEEP_METHOD, channel='test_channel'),
        pg_message(SLEEP_METHOD, channel='test_channel2'),
        pg_message(SLEEP_METHOD, channel='test_channel3'),
        pg_message(SLEEP_METHOD, channel='test_channel4'),  # not listening to this
    )
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 3


@pytest.mark.asyncio
async def test_ten_messages_queued(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.gather(*[pg_message(SLEEP_METHOD) for i in range(15)])
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 15


def get_worker_data(response_list: list[dict[str, Union[str, dict]]]) -> dict:
    "Given some control-and-response data, assuming 1 node, 1 entry, get the task message"
    assert len(response_list) == 1
    response = response_list[0].copy()
    response.pop('node_id', None)
    assert len(response) == 1
    return list(response.values())[0]


@pytest.mark.asyncio
async def test_get_running_jobs(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_me'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    running_job = get_worker_data(running_jobs)

    assert running_job['uuid'] == 'find_me'


@pytest.mark.asyncio
async def test_cancel_task(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.sleep(0.2)
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'foobar'}, timeout=1), timeout=5)
    canceled_message = get_worker_data(canceled_jobs)
    assert canceled_message['uuid'] == 'foobar'
    await asyncio.wait_for(clearing_task, timeout=3)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, apg_dispatcher.control_count] == [0, 1, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_message_with_delay(apg_dispatcher, pg_message, pg_control):
    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task had a delay")', 'uuid': 'delay_task', 'delay': 0.3})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    running_job = get_worker_data(running_jobs)
    assert running_job['uuid'] == 'delay_task'
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    pool = apg_dispatcher.pool

    # Wait for task to finish, assertions after completion
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    assert [pool.finished_count, pool.canceled_count, apg_dispatcher.control_count] == [1, 0, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_cancel_delayed_task(apg_dispatcher, pg_message, pg_control):
    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 0.8})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'delay_task_will_cancel'}, timeout=1), timeout=5)
    canceled_job = get_worker_data(canceled_jobs)
    assert canceled_job['uuid'] == 'delay_task_will_cancel'

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert list(running_jobs[0].keys()) == ['node_id']

    assert apg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_cancel_with_no_reply(apg_dispatcher, pg_message, pg_control):
    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 2.0})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await pg_control.acontrol('cancel', data={'uuid': 'delay_task_will_cancel'})
    await asyncio.sleep(0.04)

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert list(running_jobs[0].keys()) == ['node_id']

    assert apg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_alive_check(apg_dispatcher, pg_control):
    alive = await asyncio.wait_for(pg_control.acontrol_with_reply('alive', timeout=1), timeout=5)
    assert len(alive) == 1
    assert list(alive[0].keys()) == ['node_id']

    assert apg_dispatcher.control_count == 1


@pytest.mark.asyncio
async def test_aio_tasks(apg_dispatcher, pg_control):
    aio_tasks_list = await asyncio.wait_for(pg_control.acontrol_with_reply('aio_tasks', timeout=1), timeout=5)
    assert len(aio_tasks_list) == 1
    aio_tasks = aio_tasks_list[0]
    assert 'results_task' in aio_tasks
    assert aio_tasks['results_task']['done'] is False
    assert aio_tasks['results_task'].get('stack')
    assert 'No stack for' not in aio_tasks['results_task'].get('stack')
    assert apg_dispatcher.control_count == 1

    aio_tasks_list = await asyncio.wait_for(pg_control.acontrol_with_reply('aio_tasks', data={'limit': 0}, timeout=1), timeout=5)

    assert len(aio_tasks_list) == 1
    aio_tasks = aio_tasks_list[0]
    assert 'No stack for' in aio_tasks['results_task'].get('stack')
    assert apg_dispatcher.control_count == 2


@pytest.mark.asyncio
async def test_task_discard(apg_dispatcher, pg_message):
    messages = [json.dumps({'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}) for i in range(10)]

    await asyncio.gather(*[pg_message(msg) for msg in messages])

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.blocker.discard_count] == [0, 9]  # First task should still be running


@pytest.mark.asyncio
async def test_task_discard_in_task_definition(apg_dispatcher, test_settings):
    for i in range(10):
        test_methods.sleep_discard.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.blocker.discard_count] == [0, 9]  # First task should still be running


@pytest.mark.asyncio
async def test_tasks_in_serial(apg_dispatcher, test_settings):
    for i in range(10):
        test_methods.sleep_serial.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, sum(1 for w in pool.workers if w.current_task), pool.blocker.count(), pool.blocker.discard_count] == [0, 1, 9, 0]


@pytest.mark.asyncio
async def test_tasks_queue_one(apg_dispatcher, test_settings):
    for i in range(10):
        test_methods.sleep_queue_one.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, sum(1 for w in pool.workers if w.current_task), pool.blocker.count(), pool.blocker.discard_count] == [0, 1, 1, 8]


@pytest.mark.asyncio
async def test_scale_up(apg_dispatcher, test_settings):
    assert len(apg_dispatcher.pool.workers) == 1  # from conftest.py, 1 min, 6 max
    for _ in range(6):
        test_methods.sleep_function.apply_async(args=[1], settings=test_settings)

    for _ in range(20):
        await asyncio.sleep(0.01)
        if len(apg_dispatcher.pool.workers) == 6:
            break
    else:
        assert f'Never scaled up to expected 6 workers, have: {apg_dispatcher.pool.workers}'
