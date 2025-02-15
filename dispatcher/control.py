import asyncio
import json
import logging
import time
import uuid
from typing import Optional

from dispatcher.factories import get_broker
from dispatcher.producers import BrokeredProducer
from dispatcher.utils import MODULE_METHOD_DELIMITER

logger = logging.getLogger('awx.main.dispatch.control')


class ControlEvents:
    def __init__(self) -> None:
        self.exit_event = asyncio.Event()


class ControlCallbacks:
    """This calls follows the same structure as the DispatcherMain class

    it exists to interact with producers, using variables relevant to the particular
    control message being sent"""

    def __init__(self, queuename, send_data, expected_replies):
        self.queuename = queuename
        self.send_data = send_data
        self.expected_replies = expected_replies

        self.received_replies = []
        self.events = ControlEvents()
        self.shutting_down = False

    async def process_message(self, payload, producer=None, channel=None):
        self.received_replies.append(payload)
        if self.expected_replies and (len(self.received_replies) >= self.expected_replies):
            self.events.exit_event.set()

    async def connected_callback(self, producer) -> None:
        payload = json.dumps(self.send_data)
        await producer.notify(channel=self.queuename, message=payload)
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
    def __init__(self, broker_name: str, broker_config: dict, queue: Optional[str] = None) -> None:
        self.queuename = queue
        self.broker_name = broker_name
        self.broker_config = broker_config

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
        broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue])
        return BrokeredProducer(broker, close_on_exit=True)

    def sanitize_command(self, command: str) -> str:
        if MODULE_METHOD_DELIMITER not in command:
            return f'dispatcher.tasks.{command}'
        return command

    async def acontrol_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        reply_queue = Control.generate_reply_queue_name()
        send_data = {'control': self.sanitize_command(command), 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        return await self.acontrol_with_reply_internal(self.make_producer(reply_queue), send_data, expected_replies, timeout)

    async def acontrol(self, command, data=None):
        send_data = {'control': self.sanitize_command(command)}
        if data:
            send_data['control_data'] = data

        control_callbacks = ControlCallbacks(self.queuename, send_data, 0)
        producer = self.make_producer(Control.generate_reply_queue_name())  # reply queue not used
        await control_callbacks.connected_callback(producer)

    def control_with_reply(self, command, expected_replies=1, timeout=1, data=None):
        logger.info('control-and-reply {} to {}'.format(command, self.queuename))
        start = time.time()
        reply_queue = Control.generate_reply_queue_name()
        send_data = {'control': self.sanitize_command(command), 'reply_to': reply_queue}
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

    def control(self, command, data=None):
        "Send message in fire-and-forget mode, as synchronous code. Only for no-reply control."
        send_data = {'control': self.sanitize_command(command)}
        if data:
            send_data['control_data'] = data

        payload = json.dumps(send_data)
        broker = get_broker(self.broker_name, self.broker_config)
        broker.publish_message(channel=self.queuename, message=payload)
