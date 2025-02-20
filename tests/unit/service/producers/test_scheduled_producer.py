import asyncio

import pytest

from dispatcher.producers import ScheduledProducer


class ItWorked(Exception):
    pass


class Dispatcher:
    def __init__(self):
        self.test_done = asyncio.Event()

    async def process_message(self, message):
        assert message.get('on_duplicate') == 'queue_one'
        assert 'schedule' not in message
        raise ItWorked

    async def fatal_error_callback(self, *args, **kwargs):
        raise Exception('task error')


async def run_schedules_for_a_while(producer):
    dispatcher = Dispatcher()
    await producer.start_producing(dispatcher)
    for task in producer.all_tasks():
        await task


def test_scheduled_producer_with_options():
    producer = ScheduledProducer({
        'tests.data.methods.print_hello': {
            'schedule': 0.1,
            'on_duplicate': 'queue_one'
        }
    })

    loop = asyncio.get_event_loop()
    with pytest.raises(ItWorked):
        loop.run_until_complete(run_schedules_for_a_while(producer))
