import asyncio
import json
import logging
import time
import uuid
from types import SimpleNamespace

from dispatcher.factories import get_async_publisher_from_settings, get_sync_publisher_from_settings
from dispatcher.producers import BrokeredProducer

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

    async def process_message(self, payload, producer=None, channel=None):
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
    def __init__(self, queue, config=None):
        self.queuename = queue
        self.config = config

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
        broker = get_async_publisher_from_settings(channels=[reply_queue])
        return BrokeredProducer(broker, close_on_exit=True)

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

        send_data = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        producer = self.make_producer(reply_queue)

        loop = asyncio.new_event_loop()
        try:
            replies = loop.run_until_complete(self.acontrol_with_reply_internal(producer, send_data, expected_replies, timeout))
        finally:
            loop.close()
            loop = None

        logger.info(f'control-and-reply message returned in {time.time() - start} seconds')
        return replies

    # NOTE: this is the synchronous version, only to be used for no-reply
    def control(self, command, data=None):
        send_data = {'control': command}
        if data:
            send_data['control_data'] = data

        payload = json.dumps(send_data)
        broker = get_sync_publisher_from_settings()
        broker.publish_message(channel=self.queuename, message=payload)
