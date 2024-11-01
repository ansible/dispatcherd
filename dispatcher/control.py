import asyncio
import json
import logging
import time
import uuid

from dispatcher.producers.brokered import BrokeredProducer

logger = logging.getLogger('awx.main.dispatch')


class Control(object):
    def __init__(self, queue, config=None):
        self.queuename = queue
        self.config = config
        self.received_replies = []
        self.expected_replies = None
        self.exit_event = None
        self.shutting_down = False
        self.loop = None

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

        self.exit_event.set()

    async def process_message(self, payload, broker=None):
        self.received_replies.append(payload)
        if self.expected_replies and (len(self.received_replies) >= self.expected_replies):
            self.exit_event.set()

    async def acontrol_with_reply(self, producer, send_data, expected_replies, timeout):
        self.shutting_down = False
        self.received_replies = []

        self.expected_replies = expected_replies
        self.exit_event = asyncio.Event()
        await producer.start_producing(self)

        payload = json.dumps(send_data)
        await producer.notify(self.queuename, payload)

        try:
            await asyncio.wait_for(self.exit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(self.received_replies)}')

        self.shutting_down = True
        await producer.shutdown()

    def control_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        logger.info('control-and-reply {} to {}'.format(command, self.queuename))
        start = time.time()
        reply_queue = Control.generate_reply_queue_name()

        if not self.config:
            raise RuntimeError('Must use a new psycopg connection to do control-and-reply')

        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        producer = BrokeredProducer(broker='pg_notify', config=self.config, channels=[reply_queue])

        self.loop = asyncio.new_event_loop()
        try:
            self.loop.run_until_complete(self.acontrol_with_reply(producer, send_data, expected_replies, timeout))
        finally:
            self.loop.close()
            self.loop = None

        parsed_replies = [json.loads(payload) for payload in self.received_replies]
        logger.info(f'control-and-reply message returned in {time.time() - start} seconds')

        return parsed_replies

    # NOTE: this is the synchronous version, only to be used for no-reply
    def control(self, command, data=None):
        from dispatcher.brokers.pg_notify import publish_message

        send_data = {'control': command}
        if data:
            send_data['control_data'] = data

        payload = json.dumps(send_data)
        publish_message(self.queuename, payload, config=self.config)
