# send_notifications.py
import json
import logging
import os
import sys

from dispatcher.factories import get_publisher_from_settings, get_control_from_settings
from dispatcher.utils import MODULE_METHOD_DELIMITER
from dispatcher.config import setup

# Add the test methods to the path so we can use .delay type contracts
tools_dir = os.path.abspath(
    os.path.dirname(os.path.abspath(__file__)),
)

sys.path.append(tools_dir)

from test_methods import sleep_function, sleep_discard, task_has_timeout, hello_world_binder


# Setup the global config from the settings file shared with the service
setup(file_path='dispatcher.yml')


broker = get_publisher_from_settings()


TEST_MSGS = [
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
    ('test_channel2', 'lambda: __import__("time").sleep(1)'),
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
]


def main():
    print('writing some basic test messages')
    for channel, message in TEST_MSGS:
        # Send the notification
        broker.publish_message(channel=channel, message=message)
        # await send_notification(channel, message)
    # send more than number of workers quickly
    print('')
    print('writing 15 messages fast')
    for i in range(15):
        broker.publish_message(message=f'lambda: {i}')

    print('')
    print('performing a task cancel')
    # submit a task we will "find" two different ways
    broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'}))
    ctl = get_control_from_settings()
    canceled_jobs = ctl.control_with_reply('cancel', data={'uuid': 'foobar'})
    print(json.dumps(canceled_jobs, indent=2))

    print('')
    print('finding a running task by its task name')
    broker.publish_message(message=json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar2'}))
    running_data = ctl.control_with_reply('running', data={'task': 'lambda: __import__("time").sleep(3.1415)'})
    print(json.dumps(running_data, indent=2))

    print('writing a message with a delay')
    print('     4 second delay task')
    broker.publish_message(message=json.dumps({'task': 'lambda: 123421', 'uuid': 'foobar2', 'delay': 4}))
    print('     30 second delay task')
    broker.publish_message(message=json.dumps({'task': 'lambda: 987987234', 'uuid': 'foobar2', 'delay': 30}))
    print('     10 second delay task')
    # NOTE: this task will error unless you run the dispatcher itself with it in the PYTHONPATH, which is intended
    sleep_function.apply_async(
        args=[3],  # sleep 3 seconds
        delay=10,
    )

    print('')
    print('showing delayed tasks in running list')
    running_data = ctl.control_with_reply('running', data={'task': f'test_methods{MODULE_METHOD_DELIMITER}sleep_function'})
    print(json.dumps(running_data, indent=2))

    print('')
    print('cancel a delayed task with no reply for demonstration')
    ctl.control('cancel', data={'task': f'test_methods{MODULE_METHOD_DELIMITER}sleep_function'})  # NOTE: no reply
    print('confirmation that it has been canceled')
    running_data = ctl.control_with_reply('running', data={'task': f'test_methods{MODULE_METHOD_DELIMITER}sleep_function'})
    print(json.dumps(running_data, indent=2))

    print('')
    print('running alive check a few times')
    for i in range(3):
        alive = ctl.control_with_reply('alive')
        print(alive)

    print('')
    print('demo of submitting discarding tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps(
            {'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}
        ))
    print('demo of discarding task marked as discarding')
    for i in range(10):
        sleep_discard.apply_async(args=[2])
    print('demo of discarding tasks with apply_async contract')
    for i in range(10):
        sleep_function.apply_async(args=[3], on_duplicate='discard')
    print('demo of submitting waiting tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps(
            {'task': 'lambda: __import__("time").sleep(10)', 'on_duplicate': 'serial', 'uuid': f'wait-{i}'}
            ))
    print('demo of submitting queue-once tasks')
    for i in range(10):
        broker.publish_message(message=json.dumps(
            {'task': 'lambda: __import__("time").sleep(8)', 'on_duplicate': 'queue_one', 'uuid': f'queue_one-{i}'}
        ))

    print('demo of task_has_timeout that times out due to decorator use')
    task_has_timeout.delay()

    print('demo of using bind=True, with hello_world_binder')
    hello_world_binder.delay()

if __name__ == "__main__":
    logging.basicConfig(level='ERROR', stream=sys.stdout)
    main()
