import asyncio
import json
import logging
import time
import uuid
from typing import Any, Optional

from .chunking import ChunkAccumulator
from .factories import get_broker
from .protocols import Broker
from .service.asyncio_tasks import ensure_fatal

logger = logging.getLogger('awx.main.dispatch.control')


JSON_ERROR_STR = 'JSON parse error'


def _ingest_reply_payload(
    accumulator: ChunkAccumulator,
    results: list[dict],
    payload: str | dict,
    *,
    idx: int | None = None,
) -> bool:
    """Decode payload fragments and append completed replies to ``results``."""
    decoded: Optional[dict] = None
    if isinstance(payload, dict):
        decoded = payload
    else:
        try:
            candidate = json.loads(payload)
        except Exception:
            logger.warning(f"Invalid JSON for reply {idx}: {payload[:100]}, using as-is")
            results.append({'error': JSON_ERROR_STR, 'original': payload})
            return True
        if isinstance(candidate, dict):
            decoded = candidate
        else:
            logger.warning(f"Control reply {idx} decoded as non-dict: {candidate}")
            results.append({'error': JSON_ERROR_STR, 'original': payload})
            return True

    is_chunk, assembled = accumulator.ingest_dict(decoded)
    if is_chunk:
        if assembled is not None:
            results.append(assembled)
            return True
        return False

    results.append(decoded)
    return True


class BrokerCallbacks:
    def __init__(self, queuename: Optional[str], broker: Broker, send_message: str, expected_replies: int = 1) -> None:
        self.received_replies: list[dict] = []
        self.queuename = queuename
        self.broker = broker
        self.send_message = send_message
        self.expected_replies = expected_replies
        self._chunk_accumulator = ChunkAccumulator()

    async def connected_callback(self) -> None:
        await self.broker.apublish_message(channel=self.queuename, message=self.send_message)

    async def listen_for_replies(self) -> None:
        """Listen to the reply channel until we get the expected number of messages.

        This gets ran in an async task, and timing out will be accomplished by the main code
        """
        async for channel, payload in self.broker.aprocess_notify(connected_callback=self.connected_callback):
            if _ingest_reply_payload(self._chunk_accumulator, self.received_replies, payload, idx=len(self.received_replies)):
                if len(self.received_replies) >= self.expected_replies:
                    return


class SyncBrokerCallbacks:
    def __init__(self, control: 'Control', broker: Broker, message: str) -> None:
        self._control = control
        self._broker = broker
        self._message = message
        self.created_at: float = time.perf_counter()
        self.send_start: Optional[float] = None

    def connected_callback(self) -> None:
        self.send_start = time.perf_counter()
        self._broker.publish_message(channel=self._control.queuename, message=self._message)


class Control:
    def __init__(self, broker_name: str, broker_config: dict, queue: Optional[str] = None) -> None:
        self.queuename = queue
        self.broker_name = broker_name
        self.broker_config = broker_config

    def _control_broker_overrides(self) -> dict[str, Any]:
        """Return broker kwargs that disable self-checks for control actions."""
        if self.broker_name == 'pg_notify':
            return {'max_connection_idle_seconds': None, 'max_self_check_message_age_seconds': None}
        return {}

    @classmethod
    def generate_reply_queue_name(cls) -> str:
        return f"reply_to_{str(uuid.uuid4()).replace('-', '_')}"

    def create_message(self, command: str, reply_to: Optional[str] = None, send_data: Optional[dict] = None) -> str:
        to_send: dict[str, dict | str] = {'control': command}
        if reply_to:
            to_send['reply_to'] = reply_to
        if send_data:
            to_send['control_data'] = send_data
        return json.dumps(to_send)

    async def acontrol_with_reply(self, command: str, expected_replies: int = 1, timeout: int = 1, data: Optional[dict] = None) -> list[dict]:
        reply_queue = Control.generate_reply_queue_name()
        broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue], **self._control_broker_overrides())
        send_message = self.create_message(command=command, reply_to=reply_queue, send_data=data)

        control_callbacks = BrokerCallbacks(broker=broker, queuename=self.queuename, send_message=send_message, expected_replies=expected_replies)

        listen_task = asyncio.create_task(control_callbacks.listen_for_replies())
        ensure_fatal(listen_task)

        try:
            await asyncio.wait_for(listen_task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(control_callbacks.received_replies)}')
            listen_task.cancel()
            try:
                await asyncio.wait_for(listen_task, timeout=timeout)
            except asyncio.CancelledError:
                if not listen_task.cancelled():
                    raise
        finally:
            await broker.aclose()

        return control_callbacks.received_replies

    async def acontrol(self, command: str, data: Optional[dict] = None) -> None:
        broker = get_broker(self.broker_name, self.broker_config, channels=[], **self._control_broker_overrides())
        send_message = self.create_message(command=command, send_data=data)
        try:
            await broker.apublish_message(message=send_message)
        finally:
            await broker.aclose()

    def control_with_reply(self, command: str, expected_replies: int = 1, timeout: float = 1.0, data: Optional[dict] = None) -> list[dict]:
        start = time.perf_counter()
        reply_queue = Control.generate_reply_queue_name()
        send_message = self.create_message(command=command, reply_to=reply_queue, send_data=data)

        reply_accumulator = ChunkAccumulator()
        try:
            broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue], **self._control_broker_overrides())
        except TypeError:
            broker = get_broker(self.broker_name, self.broker_config, **self._control_broker_overrides())

        callbacks = SyncBrokerCallbacks(control=self, broker=broker, message=send_message)

        replies: list[dict] = []
        try:
            for channel, payload in broker.process_notify(connected_callback=callbacks.connected_callback, max_messages=None, timeout=timeout):
                if _ingest_reply_payload(reply_accumulator, replies, payload, idx=len(replies)):
                    if len(replies) >= expected_replies:
                        break
            end = time.perf_counter()
            if callbacks.send_start is not None:
                round_trip_text = f'{end - callbacks.send_start:.3f}s'
            else:
                elapsed = end - callbacks.created_at
                round_trip_text = f'n/a (connected callback unused, elapsed {elapsed:.3f}s)'
            logger.info(
                'control-and-reply %s returned in %.3f seconds (round-trip %s)',
                command,
                end - start,
                round_trip_text,
            )
            return replies
        finally:
            broker.close()

    def control(self, command: str, data: Optional[dict] = None) -> None:
        """Send a fire-and-forget control message synchronously."""
        broker = get_broker(self.broker_name, self.broker_config, **self._control_broker_overrides())
        send_message = self.create_message(command=command, send_data=data)
        try:
            broker.publish_message(channel=self.queuename, message=send_message)
        finally:
            broker.close()
