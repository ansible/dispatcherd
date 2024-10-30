# send_notifications.py
import json

import asyncio

from dispatcher.brokers.pg_notify import publish_message
from dispatcher.control import Control

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


TEST_MSGS = [
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
    ('test_channel2', 'lambda: __import__("time").sleep(1)'),
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
]


async def main():
    print('writing some basic test messages')
    for channel, message in TEST_MSGS:
        # Send the notification
        publish_message(channel, message, config={'conninfo': CONNECTION_STRING})
        # await send_notification(channel, message)
    # send more than number of workers quickly
    print('')
    print('writing 15 messages fast')
    for i in range(15):
        publish_message('test_channel', f'lambda: {i}', config={'conninfo': CONNECTION_STRING})
    print('')
    print('writing a control message')

    # submit a task we will "find" two different ways
    publish_message(channel, json.dumps({'task': 'lambda: 4', 'uuid': 'foobar'}), config={'conninfo': CONNECTION_STRING})

    ctl = Control('test_channel', config={'conninfo': CONNECTION_STRING})
    # running_data = ctl.control_with_reply('running', data={'task': 'lambda: 4'})
    # print(json.dumps(running_data, indent=2))
    running_data = ctl.control_with_reply('cancel', data={'uuid': 'foobar'})
    print(json.dumps(running_data, indent=2))

    print('writing a message with a delay')
    publish_message(channel, json.dumps({'task': 'lambda: 123421', 'uuid': 'foobar2', 'delay': 4}), config={'conninfo': CONNECTION_STRING})
    publish_message(channel, json.dumps({'task': 'lambda: 987987234', 'uuid': 'foobar2', 'delay': 30}), config={'conninfo': CONNECTION_STRING})


if __name__ == "__main__":
    asyncio.run(main())
