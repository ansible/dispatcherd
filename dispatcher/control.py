import asyncio
import json
import logging
import time
import uuid
from types import SimpleNamespace

from dispatcher.producers.brokered import BrokeredProducer

logger = logging.getLogger('awx.main.dispatch.control')


class ControlCallbacks:
    """This calls follows the same structure as the DispatcherMain class

    it exists to interact with producers, using variables relevant to the particular
    control message being sent"""

    def __init__(self, queuename, send_data, expected_replies):
        self.queuename = queuename
        self.send_data = send_data
        self.expected_replies = expected_replies

        self.received_replies = []
        self.events = self._create_events()
        self.shutting_down = False

    def _create_events(self):
        return SimpleNamespace(exit_event=asyncio.Event())

    async def process_message(self, payload, broker=None, channel=None):
        self.received_replies.append(payload)
        if self.expected_replies and (len(self.received_replies) >= self.expected_replies):
            self.events.exit_event.set()

    async def connected_callback(self, producer) -> None:
        payload = json.dumps(self.send_data)
        await producer.notify(self.queuename, payload)
        logger.info('Sent control message, expecting replies soon')

    def fatal_error_callback(self, *args):
        if self.shutting_down:
            return

        for task in args:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(f'Exception from {task.get_name()}, exit flag set')
                task._dispatcher_tb_logged = True

        self.events.exit_event.set()


class Control(object):
    def __init__(self, queue, config=None, async_connection=None):
        self.queuename = queue
        self.config = config
        self.async_connection = async_connection

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

    async def acontrol_with_reply_internal(self, producer, send_data, expected_replies, timeout):
        control_callbacks = ControlCallbacks(self.queuename, send_data, expected_replies)

        await producer.start_producing(control_callbacks)
        await producer.events.ready_event.wait()

        try:
            await asyncio.wait_for(control_callbacks.events.exit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(control_callbacks.received_replies)}')

        control_callbacks.shutting_down = True
        await producer.shutdown()

        return [json.loads(payload) for payload in control_callbacks.received_replies]

    def make_producer(self, reply_queue):
        if self.async_connection:
            conn_kwargs = {'connection': self.async_connection}
        else:
            conn_kwargs = {'config': self.config}
        return BrokeredProducer(broker='pg_notify', channels=[reply_queue], **conn_kwargs)

    async def acontrol_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        reply_queue = Control.generate_reply_queue_name()
        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        return await self.acontrol_with_reply_internal(self.make_producer(reply_queue), send_data, expected_replies, timeout)

    async def acontrol(self, command, data=None):
        send_data = {'control': command}
        if data:
            send_data['control_data'] = data

        control_callbacks = ControlCallbacks(self.queuename, send_data, 0)
        producer = self.make_producer(Control.generate_reply_queue_name())  # reply queue not used
        await control_callbacks.connected_callback(producer)

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

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.acontrol_with_reply_internal(producer, send_data, expected_replies, timeout))
        finally:
            loop.close()
            loop = None

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
