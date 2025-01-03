import time
import asyncio
import json

import pytest

from tests.data import methods as test_methods

from dispatcher.config import temporary_settings

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
    assert apg_dispatcher.pool.finished_count == 0

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


@pytest.mark.asyncio
async def test_submit_with_global_settings(apg_dispatcher, test_settings):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with temporary_settings(test_settings):
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
        pg_message(SLEEP_METHOD, channel='test_channel4')  # not listening to this
    )
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 3


@pytest.mark.asyncio
async def test_ten_messages_queued(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.gather(*[pg_message(SLEEP_METHOD) for i in range(15)])
    await asyncio.wait_for(clearing_task, timeout=3)

    assert apg_dispatcher.pool.finished_count == 15


@pytest.mark.asyncio
async def test_get_running_jobs(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_me'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    worker_id, running_job = running_jobs[0][0]

    assert running_job['uuid'] == 'find_me'


@pytest.mark.asyncio
async def test_cancel_task(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    await pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    await asyncio.sleep(0.04)
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'foobar'}, timeout=1), timeout=5)
    worker_id, canceled_message = canceled_jobs[0][0]
    assert canceled_message['uuid'] == 'foobar'
    await asyncio.wait_for(clearing_task, timeout=3)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [0, 1, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_message_with_delay(apg_dispatcher, pg_message, pg_control):
    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task had a delay")', 'uuid': 'delay_task', 'delay': 0.3})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    worker_id, running_job = running_jobs[0][0]
    assert worker_id == '<delayed>'
    assert running_job['uuid'] == 'delay_task'
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [0, 0, 1], 'cts: [finished, canceled, control]'
    # Completing the reply itself will be a work_cleared event, so we have to clear the event
    apg_dispatcher.pool.events.work_cleared.clear()

    # Wait for task to finish, assertions after completion
    await asyncio.wait_for(apg_dispatcher.pool.events.work_cleared.wait(), timeout=3)
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [1, 0, 1], 'cts: [finished, canceled, control]'


@pytest.mark.asyncio
async def test_cancel_delayed_task(apg_dispatcher, pg_message, pg_control):
    assert apg_dispatcher.pool.finished_count == 0

    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 0.8})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await asyncio.sleep(0.04)
    canceled_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('cancel', data={'uuid': 'delay_task_will_cancel'}, timeout=1), timeout=5)
    worker_id, canceled_job = canceled_jobs[0][0]
    assert worker_id == '<delayed>'
    assert canceled_job['uuid'] == 'delay_task_will_cancel'

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert running_jobs == [[]]

    assert apg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_cancel_with_no_reply(apg_dispatcher, pg_message, pg_control):
    assert apg_dispatcher.pool.finished_count == 0

    # Send message to run task with a delay
    msg = json.dumps({'task': 'lambda: print("This task should be canceled before start")', 'uuid': 'delay_task_will_cancel', 'delay': 2.0})
    await pg_message(msg)

    # Make assertions while task is in the delaying phase
    await pg_control.acontrol('cancel', data={'uuid': 'delay_task_will_cancel'})
    await asyncio.sleep(0.04)

    running_jobs = await asyncio.wait_for(pg_control.acontrol_with_reply('running', timeout=1), timeout=5)
    assert running_jobs == [[]]

    assert apg_dispatcher.pool.finished_count == 0


@pytest.mark.asyncio
async def test_alive_check(apg_dispatcher, pg_control):
    assert apg_dispatcher.control_count == 0

    alive = await asyncio.wait_for(pg_control.acontrol_with_reply('alive', timeout=1), timeout=5)
    assert alive == [None]

    assert apg_dispatcher.control_count == 1


@pytest.mark.asyncio
async def test_task_discard(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    messages = [
        json.dumps(
            {'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}
        ) for i in range(10)
    ]

    await asyncio.gather(*[pg_message(msg) for msg in messages])

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.discard_count] == [0, 9]  # First task should still be running


@pytest.mark.asyncio
async def test_task_discard_in_task_definition(apg_dispatcher, test_settings):
    assert apg_dispatcher.pool.finished_count == 0

    for i in range(10):
        test_methods.sleep_discard.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.discard_count] == [0, 9]  # First task should still be running


@pytest.mark.asyncio
async def test_tasks_in_serial(apg_dispatcher, test_settings):
    assert apg_dispatcher.pool.finished_count == 0

    for i in range(10):
        test_methods.sleep_serial.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, sum(1 for w in pool.workers.values() if w.current_task), len(pool.queued_messages), pool.discard_count] == [0, 1, 9, 0]


@pytest.mark.asyncio
async def test_tasks_queue_one(apg_dispatcher, test_settings):
    assert apg_dispatcher.pool.finished_count == 0

    for i in range(10):
        test_methods.sleep_queue_one.apply_async(args=[2], settings=test_settings)

    await wait_to_receive(apg_dispatcher, 10)

    pool = apg_dispatcher.pool
    assert [pool.finished_count, sum(1 for w in pool.workers.values() if w.current_task), len(pool.queued_messages), pool.discard_count] == [0, 1, 1, 8]
