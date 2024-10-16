# send_notifications.py
import asyncio

from dispatcher.brokers.pg_notify import publish_message

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


TEST_MSGS = [
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
    ('test_channel2', 'lambda: __import__("time").sleep(1)'),
    ('test_channel', 'lambda: __import__("time").sleep(1)'),
]


async def main():
    for channel, message in TEST_MSGS:
        # Send the notification
        publish_message(channel, message, config={'conninfo': CONNECTION_STRING})
        # await send_notification(channel, message)
    # send more than number of workers quickly
    for i in range(15):
        publish_message('test_channel', f'lambda: {i}', config={'conninfo': CONNECTION_STRING})


if __name__ == "__main__":
    asyncio.run(main())
