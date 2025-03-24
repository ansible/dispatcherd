#!/usr/bin/env python3

import json
import logging
import sys

from dispatcherd.config import setup
from dispatcherd.factories import get_control_from_settings, get_publisher_from_settings
from dispatcherd.utils import MODULE_METHOD_DELIMITER
from tests.data.methods import hello_world_binder, sleep_discard, sleep_function, task_has_timeout

# Setup the global config from the settings file shared with the service
setup(file_path='dispatcher.yml')


broker = get_publisher_from_settings()


TEST_MSGS = [
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
    ('test_channel2', 'lambda: __import__("time").sleep(1)'),
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
]


# If we run against more than 1 background task service,
# we need to wait for that-many replies, so allow this via sys.argv last arg
expected_count = 1
try:
    expected_count = int(sys.argv[-1])
except ValueError:
    pass


def main():
    print('writing some basic test messages')
    for channel, message in TEST_MSGS:
        broker.publish_message(channel=channel, message=message)

    # send more than number of workers quickly
    print('')
    print('writing 15 messages fast')
    for i in range(15):
        broker.publish_message(message=f'lambda: {i}')

    print('')
    print(' -------- Actions involving control-and-reply ---------')
    ctl = get_control_from_settings()

    print('')
    print('performing a task cancel')
    # we will "find" a task two different ways here
    broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'}))
    canceled_jobs = ctl.control_with_reply('cancel', data={'uuid': 'foobar'}, expected_replies=expected_count)
    print(json.dumps(canceled_jobs, indent=2))

    print('')
    print('finding a running task by its task name')
    broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'find_by_name'}))
    running_data = ctl.control_with_reply('running', data={'task': 'lambda: __import__("time").sleep(3.1415)'}, expected_replies=expected_count)
    print(json.dumps(running_data, indent=2))

    print('')
    print('getting worker status')
    worker_data = ctl.control_with_reply('workers', expected_replies=expected_count)
    print(json.dumps(worker_data, indent=2))

    print('')
    print('getting main process tasks')
    task_data_list = ctl.control_with_reply('aio_tasks')
    for orig_task_data in task_data_list:
        task_data = orig_task_data.copy()
        print(f'Task data for node {task_data["node_id"]}')
        task_data.pop('node_id', None)
        for task_name, aio_task_data in task_data.items():
            print(f'  {task_name} is done={aio_task_data["done"]}')
            print('   trace:')
            print(aio_task_data['stack'])

    print('')
    print('run bogus control command')
    worker_data = ctl.control_with_reply('not-a-command', expected_replies=expected_count)
    print(json.dumps(worker_data, indent=2))

    print('writing a message with a delay')
    print('     4 second delay task')
    broker.publish_message(message=json.dumps({'task': 'lambda: 123421', 'uuid': 'delay_4', 'delay': 4}))
    print('     30 second delay task')
    broker.publish_message(message=json.dumps({'task': 'lambda: 987987234', 'uuid': 'delay_30', 'delay': 30}))

    print('')
    print(' -------- Using tasks defined with @task() decorator ---------')
    print('')
    print('running tests.data.methods.sleep_function with a delay')
    print('     10 second delay task')
    # NOTE: this task will error unless you run the dispatcherd itself with it in the PYTHONPATH, which is intended
    sleep_function.apply_async(
        args=[3],  # sleep 3 seconds
        delay=10,
    )

    print('')
    print('showing delayed tasks in running list')
    running_data = ctl.control_with_reply(
        'running', data={'task': f'tests.data.methods{MODULE_METHOD_DELIMITER}sleep_function'}, expected_replies=expected_count
    )
    print(json.dumps(running_data, indent=2))

    print('')
    print('cancel a delayed task with no reply for demonstration')
    ctl.control('cancel', data={'task': f'tests.data.methods{MODULE_METHOD_DELIMITER}sleep_function'})  # NOTE: no reply
    print('confirmation that it has been canceled')
    running_data = ctl.control_with_reply(
        'running', data={'task': f'tests.data.methods{MODULE_METHOD_DELIMITER}sleep_function'}, expected_replies=expected_count
    )
    print(json.dumps(running_data, indent=2))

    print('')
    print('running alive check a few times')
    for i in range(3):
        alive = ctl.control_with_reply('alive', expected_replies=expected_count)
        print(alive)

    print('')
    print('demo of submitting discarding tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}))
    print('demo of discarding task marked as discarding')
    for i in range(10):
        sleep_discard.apply_async(args=[2])
    print('demo of discarding tasks with apply_async contract')
    for i in range(10):
        sleep_function.apply_async(args=[3], on_duplicate='discard')
    print('demo of submitting waiting tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(10)', 'on_duplicate': 'serial', 'uuid': f'wait-{i}'}))
    print('demo of submitting queue-once tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(8)', 'on_duplicate': 'queue_one', 'uuid': f'queue_one-{i}'}))

    print('demo of task_has_timeout that times out due to decorator use')
    task_has_timeout.delay()

    print('demo of using bind=True, with hello_world_binder')
    hello_world_binder.delay()


if __name__ == "__main__":
    logging.basicConfig(level='ERROR', stream=sys.stdout)
    main()
