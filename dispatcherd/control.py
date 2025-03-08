import asyncio
import json
import logging
import time
import uuid
from typing import Optional, Union

from .factories import get_broker
from .protocols import Broker
from .service.asyncio_tasks import ensure_fatal

logger = logging.getLogger('awx.main.dispatch.control')


class BrokerCallbacks:
    def __init__(self, queuename: Optional[str], broker: Broker, send_message: str, expected_replies: int = 1) -> None:
        self.received_replies: list[str] = []
        self.queuename = queuename
        self.broker = broker
        self.send_message = send_message
        self.expected_replies = expected_replies

    async def connected_callback(self) -> None:
        await self.broker.apublish_message(channel=self.queuename, message=self.send_message)

    async def listen_for_replies(self) -> None:
        """Listen to the reply channel until we get the expected number of messages.

        This gets ran in an async task, and timing out will be accomplished by the main code
        """
        async for channel, payload in self.broker.aprocess_notify(connected_callback=self.connected_callback):
            self.received_replies.append(payload)
            if len(self.received_replies) >= self.expected_replies:
                return


class Control:
    def __init__(self, broker_name: str, broker_config: dict, queue: Optional[str] = None) -> None:
        self.queuename = queue
        self.broker_name = broker_name
        self.broker_config = broker_config

    @classmethod
    def generate_reply_queue_name(cls) -> str:
        return f"reply_to_{str(uuid.uuid4()).replace('-', '_')}"

    @staticmethod
    def parse_replies(received_replies: list[str]) -> list[dict]:
        ret = []
        for i, payload in enumerate(received_replies):
            try:
                item_as_dict = json.loads(payload)
                ret.append(item_as_dict)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON for reply for reply {i}: {payload[:100]}... (Error: {e})")
                ret.append({'error': 'JSON parse error', 'original': payload})

        return ret

    def create_message(self, command: str, reply_to: Optional[str] = None, send_data: Optional[dict] = None) -> str:
        to_send: dict[str, Union[dict, str]] = {'control': command}
        if reply_to:
            to_send['reply_to'] = reply_to
        if send_data:
            to_send['control_data'] = send_data
        return json.dumps(to_send)

    async def acontrol_with_reply(self, command: str, expected_replies: int = 1, timeout: int = 1, data: Optional[dict] = None) -> list[dict]:
        reply_queue = Control.generate_reply_queue_name()
        broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue])
        send_message = self.create_message(command=command, reply_to=reply_queue, send_data=data)

        control_callbacks = BrokerCallbacks(broker=broker, queuename=self.queuename, send_message=send_message, expected_replies=expected_replies)

        listen_task = asyncio.create_task(control_callbacks.listen_for_replies())
        ensure_fatal(listen_task)

        try:
            await asyncio.wait_for(listen_task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(control_callbacks.received_replies)}')
            listen_task.cancel()
        finally:
            await broker.aclose()

        return self.parse_replies(control_callbacks.received_replies)

    async def acontrol(self, command: str, data: Optional[dict] = None) -> None:
        broker = get_broker(self.broker_name, self.broker_config, channels=[])
        send_message = self.create_message(command=command, send_data=data)
        try:
            await broker.apublish_message(message=send_message)
        finally:
            await broker.aclose()

    def control_with_reply(self, command: str, expected_replies: int = 1, timeout: float = 1.0, data: Optional[dict] = None) -> list[dict]:
        start = time.time()
        reply_queue = Control.generate_reply_queue_name()
        send_message = self.create_message(command=command, reply_to=reply_queue, send_data=data)

        try:
            broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue])
        except TypeError:
            broker = get_broker(self.broker_name, self.broker_config)

        def connected_callback() -> None:
            broker.publish_message(channel=self.queuename, message=send_message)

        replies = []
        try:
            for channel, payload in broker.process_notify(connected_callback=connected_callback, max_messages=expected_replies, timeout=timeout):
                reply_data = json.loads(payload)
                replies.append(reply_data)
            logger.info(f'control-and-reply message returned in {time.time() - start} seconds')
            return replies
        finally:
            broker.close()

    def control(self, command: str, data: Optional[dict] = None) -> None:
        """Send a fire-and-forget control message synchronously."""
        broker = get_broker(self.broker_name, self.broker_config)
        send_message = self.create_message(command=command, send_data=data)
        try:
            broker.publish_message(channel=self.queuename, message=send_message)
        finally:
            broker.close()
