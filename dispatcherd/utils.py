import importlib
import json
import logging
import uuid
from enum import Enum
from typing import Callable, Dict, Optional, Protocol, Type, Union, runtime_checkable


@runtime_checkable
class RunnableClass(Protocol):
    def run(self, *args, **kwargs) -> None: ...


MODULE_METHOD_DELIMITER = '.'


DispatcherCallable = Union[Callable, Type[RunnableClass]]


def resolve_callable(task: str) -> Optional[Callable]:
    """
    Transform a dotted notation task into an imported, callable function, e.g.,

    awx.main.tasks.system.delete_inventory
    awx.main.tasks.jobs.RunProjectUpdate

    In AWX this also did validation that the method was marked as a task.
    That is out of scope of this method now.
    This is mainly used by the worker.
    """
    if task.startswith('lambda'):
        return eval(task)

    if MODULE_METHOD_DELIMITER not in task:
        raise RuntimeError(f'Given task name can not be parsed as task {task}')

    module_name, target = task.rsplit(MODULE_METHOD_DELIMITER, 1)
    module = importlib.import_module(module_name)
    _call = None
    if hasattr(module, target):
        _call = getattr(module, target, None)

    return _call


def serialize_task(f: Callable) -> str:
    """The reverse of resolve_callable, transform callable into dotted notation"""
    return MODULE_METHOD_DELIMITER.join([f.__module__, f.__name__])


class DuplicateBehavior(Enum):
    parallel = 'parallel'  # run multiple versions of same task at same time
    discard = 'discard'  # if task is submitted twice, discard the 2nd one
    serial = 'serial'  # hold duplicate submissions in queue but only run 1 at a time
    queue_one = 'queue_one'  # hold only 1 duplicate submission in queue, discard any more


logger = logging.getLogger(__name__)

CHUNK_MARKER = '__dispatcherd_chunk__'
CHUNK_VERSION = 'dispatcherd.v1'
DEFAULT_HEADER_RESERVE = 256


class MessageChunker:
    """Utility for splitting broker messages into chunk envelopes."""

    def __init__(self, max_bytes: int | None = None, header_reserve: int = DEFAULT_HEADER_RESERVE) -> None:
        self.max_bytes = max_bytes
        self.header_reserve = header_reserve

    def _serialize_chunk(self, chunk_id: str, seq: int, is_final: bool, payload: str) -> str:
        chunk = {
            CHUNK_MARKER: CHUNK_VERSION,
            'message_id': chunk_id,
            'chunk_index': seq,
            'final_chunk': is_final,
            'payload': payload,
        }
        return json.dumps(chunk, separators=(',', ':'))

    def split(self, message: str) -> list[str]:
        """Return the message split into <= max_bytes sized chunks."""
        if (self.max_bytes is None) or (len(message.encode('utf-8')) <= self.max_bytes):
            return [message]

        if self.max_bytes <= self.header_reserve:
            raise ValueError('max_bytes must be larger than header reserve to enable chunking')

        payload_limit = max(1, self.max_bytes - self.header_reserve)
        chunk_id = uuid.uuid4().hex

        chunks: list[str] = []
        msg_len = len(message)
        idx = 0
        seq = 0
        while idx < msg_len:
            chunk_chars: list[str] = []
            chunk_bytes = 0
            # Build up a payload respecting the payload_limit in bytes.
            while idx < msg_len:
                char = message[idx]
                encoded_char = char.encode('utf-8')
                if chunk_bytes + len(encoded_char) > payload_limit:
                    if not chunk_chars:
                        raise ValueError('Unable to fit a single character inside configured payload limit')
                    break
                chunk_chars.append(char)
                chunk_bytes += len(encoded_char)
                idx += 1

            if not chunk_chars:
                raise RuntimeError('Chunk preparation created an empty payload, aborting')

            chunk_payload = ''.join(chunk_chars)
            is_final = idx >= msg_len
            chunk_str = self._serialize_chunk(chunk_id, seq, is_final, chunk_payload)
            encoded_chunk = chunk_str.encode('utf-8')

            # Metadata grows with the sequence/index digits, so trim until the chunk fits.
            while len(encoded_chunk) > self.max_bytes and chunk_chars:
                idx -= 1
                chunk_chars.pop()
                chunk_payload = ''.join(chunk_chars)
                is_final = idx >= msg_len
                chunk_str = self._serialize_chunk(chunk_id, seq, is_final, chunk_payload)
                encoded_chunk = chunk_str.encode('utf-8')

            if len(encoded_chunk) > self.max_bytes:
                raise RuntimeError('Chunk metadata exceeds the configured max bytes limit')

            chunks.append(chunk_str)
            seq += 1

        return chunks

    @staticmethod
    def parse_chunk(payload: str) -> Optional[dict]:
        try:
            decoded = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return None

        if not isinstance(decoded, dict):
            return None
        if decoded.get(CHUNK_MARKER) != CHUNK_VERSION:
            return None
        return decoded


class ChunkAccumulator:
    """Track partial chunked messages keyed by message id."""

    def __init__(self) -> None:
        self.pending_messages: Dict[str, Dict[int, str]] = {}
        self.final_indexes: Dict[str, int] = {}

    def ingest(self, payload: str) -> tuple[bool, Optional[str], Optional[str]]:
        """Process a payload and return (is_chunk, completed_payload, message_id)."""
        chunk = MessageChunker.parse_chunk(payload)
        if not chunk:
            return (False, payload, None)

        message_id = chunk.get('message_id') or chunk.get('id')
        seq = chunk.get('chunk_index')
        is_final = chunk.get('final_chunk')
        if seq is None:
            seq = chunk.get('seq')
        if is_final is None:
            is_final = chunk.get('final')

        if not isinstance(message_id, str) or not isinstance(seq, int):
            logger.warning('Received chunk with invalid metadata: %s', chunk)
            return (True, None, None)

        payload_str = chunk.get('payload', '')
        if not isinstance(payload_str, str):
            payload_str = str(payload_str)

        buffer = self.pending_messages.setdefault(message_id, {})
        buffer[seq] = payload_str

        if bool(is_final):
            self.final_indexes[message_id] = seq

        final_seq = self.final_indexes.get(message_id)
        if final_seq is None:
            return (True, None, message_id)

        if any(index not in buffer for index in range(final_seq + 1)):
            return (True, None, message_id)

        message = ''.join(buffer[index] for index in range(final_seq + 1))
        self.pending_messages.pop(message_id, None)
        self.final_indexes.pop(message_id, None)
        return (True, message, message_id)

    def clear(self) -> None:
        self.pending_messages.clear()
        self.final_indexes.clear()


class JsonChunkStream:
    """Incrementally extract discrete JSON strings from an incoming text stream."""

    def __init__(self) -> None:
        self._decoder = json.JSONDecoder()
        self._buffer = ''

    def feed(self, data: str) -> list[str]:
        """Return a list of complete JSON strings extracted from the stream."""
        if data:
            self._buffer += data
        complete: list[str] = []
        while self._buffer:
            stripped_buffer = self._buffer.lstrip()
            if stripped_buffer != self._buffer:
                self._buffer = stripped_buffer
                if not self._buffer:
                    break
            try:
                _, index = self._decoder.raw_decode(self._buffer)
            except json.JSONDecodeError:
                break
            chunk_str = self._buffer[:index]
            complete.append(chunk_str)
            self._buffer = self._buffer[index:]
        return complete

    @property
    def pending(self) -> str:
        """Return any buffered partial JSON data."""
        return self._buffer

    def clear(self) -> None:
        self._buffer = ''
