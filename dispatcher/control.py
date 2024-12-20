import asyncio
import json
import logging
import time
import uuid
from types import SimpleNamespace

from dispatcher.producers.brokered import BrokeredProducer

logger = logging.getLogger('awx.main.dispatch')


class Control(object):
    def __init__(self, queue, config=None, async_connection=None):
        self.queuename = queue
        self.config = config
        self.async_connection = async_connection
        self.received_replies = []
        self.expected_replies = None
        self.shutting_down = False
        self.loop = None

        self.events = self._create_events()

    def _create_events(self):
        return SimpleNamespace(exit_event=asyncio.Event())

    def running(self, *args, **kwargs):
        return self.control_with_reply('running', *args, **kwargs)

    def cancel(self, task_ids, with_reply=True):
        if with_reply:
            return self.control_with_reply('cancel', extra_data={'task_ids': task_ids})
        else:
            self.control({'control': 'cancel', 'task_ids': task_ids, 'reply_to': None}, extra_data={'task_ids': task_ids})

    @classmethod
    def generate_reply_queue_name(cls):
        return f"reply_to_{str(uuid.uuid4()).replace('-', '_')}"

    def fatal_error_callback(self, *args):
        if self.shutting_down:
            return

        for task in args:
            try:
                task.result()
            except Exception:
                logger.exception(f'Exception from {task.get_name()}, exit flag set')
                task._dispatcher_tb_logged = True

        self.events.exit_event.set()

    async def process_message(self, payload, broker=None, channel=None):
        self.received_replies.append(payload)
        if self.expected_replies and (len(self.received_replies) >= self.expected_replies):
            self.events.exit_event.set()

    async def acontrol_with_reply_internal(self, producer, send_data, expected_replies, timeout):
        self.shutting_down = False
        self.received_replies = []

        self.expected_replies = expected_replies
        self.events.exit_event = asyncio.Event()
        await producer.start_producing(self)
        await producer.events.ready_event.wait()

        payload = json.dumps(send_data)
        await producer.notify(self.queuename, payload)

        try:
            await asyncio.wait_for(self.events.exit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(self.received_replies)}')

        self.shutting_down = True
        await producer.shutdown()

    def make_producer(self, reply_queue):
        if self.async_connection:
            conn_kwargs = {'connection': self.async_connection}
        else:
            conn_kwargs = {'config': self.config}
        return BrokeredProducer(broker='pg_notify', channels=[reply_queue], **conn_kwargs)

    @property
    def parsed_replies(self):
        return [json.loads(payload) for payload in self.received_replies]

    async def acontrol_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        reply_queue = Control.generate_reply_queue_name()
        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        await self.acontrol_with_reply_internal(self.make_producer(reply_queue), send_data, expected_replies, timeout)

        return self.parsed_replies

    def control_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        logger.info('control-and-reply {} to {}'.format(command, self.queuename))
        start = time.time()
        reply_queue = Control.generate_reply_queue_name()

        if (not self.config) and (not self.async_connection):
            raise RuntimeError('Must use a new psycopg connection to do control-and-reply')

        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        producer = self.make_producer(reply_queue)

        self.loop = asyncio.new_event_loop()
        try:
            self.loop.run_until_complete(self.acontrol_with_reply_internal(producer, send_data, expected_replies, timeout))
        finally:
            self.loop.close()
            self.loop = None

        logger.info(f'control-and-reply message returned in {time.time() - start} seconds')
        return self.parsed_replies

    # NOTE: this is the synchronous version, only to be used for no-reply
    def control(self, command, data=None):
        from dispatcher.brokers.pg_notify import publish_message

        send_data = {'control': command}
        if data:
            send_data['control_data'] = data

        payload = json.dumps(send_data)
        publish_message(self.queuename, payload, config=self.config)
