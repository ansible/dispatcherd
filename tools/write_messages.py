# send_notifications.py
import json
import logging
import os
import sys

from dispatcher.brokers.pg_notify import publish_message
from dispatcher.control import Control

# Add the test methods to the path so we can use .delay type contracts
tools_dir = os.path.abspath(
    os.path.dirname(os.path.abspath(__file__)),
)

sys.path.append(tools_dir)

from test_methods import print_hello, sleep_function, sleep_discard

import os, sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from test_work.math import fibonacci

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


TEST_MSGS = [
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
    ('test_channel2', 'lambda: __import__("time").sleep(1)'),
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
]


def main():
    print('writing some basic test messages')
    for channel, message in TEST_MSGS:
        # Send the notification
        publish_message(channel, message, config={'conninfo': CONNECTION_STRING})
        # await send_notification(channel, message)

    fibonacci.apply_async(args=[29], config={'conninfo': CONNECTION_STRING})

    # send more than number of workers quickly
    print('')
    print('writing 15 messages fast')
    for i in range(15):
        publish_message('test_channel', f'lambda: {i}', config={'conninfo': CONNECTION_STRING})

    print('')
    print('performing a task cancel')
    # submit a task we will "find" two different ways
    publish_message(channel, json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'}), config={'conninfo': CONNECTION_STRING})
    ctl = Control('test_channel', config={'conninfo': CONNECTION_STRING})
    canceled_jobs = ctl.control_with_reply('cancel', data={'uuid': 'foobar'})
    print(json.dumps(canceled_jobs, indent=2))

    print('')
    print('finding a running task by its task name')
    publish_message(channel, json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar2'}), config={'conninfo': CONNECTION_STRING})
    running_data = ctl.control_with_reply('running', data={'task': 'lambda: __import__("time").sleep(3.1415)'})
    print(json.dumps(running_data, indent=2))

    print('writing a message with a delay')
    print('     4 second delay task')
    publish_message(channel, json.dumps({'task': 'lambda: 123421', 'uuid': 'foobar2', 'delay': 4}), config={'conninfo': CONNECTION_STRING})
    print('     30 second delay task')
    publish_message(channel, json.dumps({'task': 'lambda: 987987234', 'uuid': 'foobar2', 'delay': 30}), config={'conninfo': CONNECTION_STRING})
    print('     10 second delay task')
    # NOTE: this task will error unless you run the dispatcher itself with it in the PYTHONPATH, which is intended
    sleep_function.apply_async(
        args=[3],  # sleep 3 seconds
        delay=10,
        config={'conninfo': CONNECTION_STRING}
    )

    print('')
    print('showing delayed tasks in running list')
    running_data = ctl.control_with_reply('running', data={'task': 'test_methods.sleep_function'})
    print(json.dumps(running_data, indent=2))

    print('')
    print('cancel a delayed task with no reply for demonstration')
    ctl.control('cancel', data={'task': 'test_methods.sleep_function'})  # NOTE: no reply
    print('confirmation that it has been canceled')
    running_data = ctl.control_with_reply('running', data={'task': 'test_methods.sleep_function'})
    print(json.dumps(running_data, indent=2))

    print('')
    print('running alive check a few times')
    for i in range(3):
        alive = ctl.control_with_reply('alive')
        print(alive)

    print('')
    print('demo of submitting discarding tasks')
    for i in range(10):
        publish_message(channel, json.dumps(
            {'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}
        ), config={'conninfo': CONNECTION_STRING})
    print('demo of discarding task marked as discarding')
    for i in range(10):
        sleep_discard.apply_async(args=[2], config={'conninfo': CONNECTION_STRING})
    print('demo of discarding tasks with apply_async contract')
    for i in range(10):
        sleep_function.apply_async(args=[3], on_duplicate='discard', config={'conninfo': CONNECTION_STRING})
    print('demo of submitting waiting tasks')
    for i in range(10):
        publish_message(channel, json.dumps(
            {'task': 'lambda: __import__("time").sleep(10)', 'on_duplicate': 'serial', 'uuid': f'wait-{i}'}
            ), config={'conninfo': CONNECTION_STRING})
    print('demo of submitting queue-once tasks')
    for i in range(10):
        publish_message(channel, json.dumps(
            {'task': 'lambda: __import__("time").sleep(8)', 'on_duplicate': 'queue_one', 'uuid': f'queue_one-{i}'}
        ), config={'conninfo': CONNECTION_STRING})

if __name__ == "__main__":
    logging.basicConfig(level='ERROR', stream=sys.stdout)
    main()
