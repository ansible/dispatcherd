"""Message chunking utilities shared across dispatcherd components.

Typical usage is a two-step process:

1. A producer (e.g., a broker implementation) calls :func:`split_message` on the
   JSON string it intends to send. Each returned chunk is itself a valid JSON
   document that includes metadata describing the parent message.
2. A consumer (e.g., :class:`dispatcherd.service.main.DispatcherMain`) creates a
    single :class:`ChunkAccumulator` instance and feeds every decoded JSON dict to
    :meth:`ChunkAccumulator.ingest_dict`. Once all chunks for a message arrive,
    the accumulator returns the fully reconstructed message dict.
"""

import asyncio
import json
import logging
import time
import uuid
from functools import lru_cache
from typing import Dict, Optional

logger = logging.getLogger(__name__)

CHUNK_MARKER = '__dispatcherd_chunk__'
CHUNK_VERSION = 'v1'


def _serialize_chunk(chunk_id: str, seq: int, total: int, payload: str) -> str:
    chunk = {
        CHUNK_MARKER: CHUNK_VERSION,
        'message_id': chunk_id,
        'index': seq,
        'total': total,
        'payload': payload,
    }
    return json.dumps(chunk, separators=(',', ':'))


def _wrapper_overhead_bytes(message_id: str, index: int, total_chunks: int) -> int:
    """Estimate bytes added by chunk metadata for the given index."""
    empty_chunk = _serialize_chunk(message_id, index, total_chunks, '')
    return len(empty_chunk.encode('utf-8'))


@lru_cache(maxsize=512)
def _escaped_char_bytes(character: str) -> int:
    """Return byte length impact of JSON-encoding a single character."""
    if len(character) != 1:
        raise ValueError('Function expects a single character input')
    encoded = json.dumps(character)
    return len(encoded[1:-1].encode('utf-8'))


def split_message(message: str, *, max_bytes: int | None = None) -> list[str]:
    """Split ``message`` into JSON chunks that respect ``max_bytes`` limits.

    Parameters
    ----------
    message:
        String to split.
    max_bytes:
        Maximum size (in bytes) allowed for each chunk. ``None`` disables
        chunking and returns the original message.

    Returns
    -------
    list[str]
        One or more JSON strings ready to send.

    Example
    -------
    >>> split_message('{"data":"' + 'x' * 30 + '"}', max_bytes=80)
    [
        '{"__dispatcherd_chunk__":"v1","message_id":"...","index":0,"total":2,"payload":"{\\"data\\":\\"xxxxxxxxxxxxxxxxxxxx\\"}"}',
        '{"__dispatcherd_chunk__":"v1","message_id":"...","index":1,"total":2,"payload":"{\\"data\\":\\"xxxxxxxxxxxx\\"}"}',
    ]
    """
    if max_bytes is None:
        return [message]

    message_byte_length = len(message.encode('utf-8'))
    if message_byte_length <= max_bytes:
        return [message]

    message_id = uuid.uuid4().hex
    total_chars = len(message)
    # Overhead is worst-case, because we can not have more chunks than there are bytes
    overhead = _wrapper_overhead_bytes(message_id, message_byte_length, message_byte_length)

    payload_budget = max_bytes - overhead
    if payload_budget <= 0:
        raise ValueError('max_bytes too small to contain chunk metadata')
    if payload_budget < 12:
        # `_escaped_char_bytes` tops out at 12 bytes for astral plane codepoints, the largest unicode char
        raise ValueError('max_bytes too small to encode payload characters')

    chunk_payloads: list[str] = []
    chunk_start = 0
    payload_bytes = 0
    char_pos = 0
    while char_pos <= total_chars:
        is_final = bool(char_pos == total_chars)
        char_size = 0  # unused during forced final flush
        if not is_final:
            char = message[char_pos]
            char_size = _escaped_char_bytes(char)

        if is_final or (payload_bytes + char_size > payload_budget):
            chunk_payload = message[chunk_start:char_pos]
            chunk_payloads.append(chunk_payload)
            # Reset the per-chunk variables
            chunk_start = char_pos
            payload_bytes = 0
            if is_final:
                break
            continue  # current character is saved for next chunk

        payload_bytes += char_size
        char_pos += 1

    total_chunks = len(chunk_payloads)
    chunks: list[str] = []
    for index, chunk_payload in enumerate(chunk_payloads):
        chunk_str = _serialize_chunk(message_id, index, total_chunks, chunk_payload)
        encoded_chunk = chunk_str.encode('utf-8')
        if len(encoded_chunk) > max_bytes:
            raise RuntimeError(f'Chunk metadata {len(encoded_chunk)} exceeds the configured max bytes limit {max_bytes}')
        chunks.append(chunk_str)

    return chunks


def parse_chunk_dict(candidate: dict) -> Optional[dict]:
    """Return the candidate dict when it matches the chunk envelope schema."""
    if not isinstance(candidate, dict):
        return None
    if CHUNK_MARKER not in candidate:
        return None
    return candidate


class ChunkAccumulator:
    """Consumer-side helper for reassembling message chunks.

    Create one accumulator per dispatcher (or per consumer) and feed every
    decoded JSON dict to :meth:`ingest_dict`.  The method returns a tuple:

    ``(is_chunk, completed_message)``

    * ``is_chunk`` indicates whether the payload was part of the chunking
      protocol.
    * ``completed_message`` is the reconstructed dict when the final chunk has
      been seen; otherwise it is ``None``.
    """

    def __init__(self, *, message_timeout_seconds: float = 30 * 60) -> None:
        self.message_timeout_seconds = message_timeout_seconds
        self.pending_messages: Dict[str, Dict[int, str]] = {}
        self.expected_totals: Dict[str, int] = {}
        self.assembly_started_at: Dict[str, float] = {}
        self.total_chunks_received = 0
        self.successful_assemblies = 0
        self.errored_assemblies = 0
        self.timed_out_assemblies = 0
        self._lock = asyncio.Lock()

    async def aingest_dict(self, payload_dict: dict) -> tuple[bool, Optional[dict]]:
        """Async wrapper that serializes :meth:`ingest_dict` mutations."""
        async with self._lock:
            return self.ingest_dict(payload_dict)

    def ingest_dict(self, payload_dict: dict) -> tuple[bool, Optional[dict]]:
        """Process a decoded payload dict and assemble chunked messages.

        Returns (message is chunked True or False, final message as dict)

        Scenarios
        ---------
        1. Payload is not chunked: returns ``(False, payload_dict)`` so callers
           can process it immediately.
        2. Chunk received but more pieces pending: returns ``(True, None)`` and logs
           chunk progress internally.
        3. Final chunk completes the message: returns ``(True, completed_dict)``
           with the assembled payload ready for processing.
        4. Chunk metadata or version invalid: returns ``(True, None)`` after logging why
           the chunk could not be associated with a message.
        5. Reassembly fails JSON validation or is not a dict: returns ``(True, None)``
           after logging the failure with message metadata.
        """
        chunk = parse_chunk_dict(payload_dict)
        if not chunk:
            return (False, payload_dict)

        self.total_chunks_received += 1

        if chunk.get(CHUNK_MARKER) != CHUNK_VERSION:
            logger.error('Unsupported chunk version: %s', chunk.get(CHUNK_MARKER))
            self.errored_assemblies += 1
            return (True, None)

        # Unpack chunk message into local vars
        message_id = chunk.get('message_id')
        seq = chunk.get('index')
        total = chunk.get('total')
        payload_str = chunk.get('payload', '')

        if not isinstance(message_id, str) or not isinstance(seq, int) or not isinstance(total, int) or not isinstance(payload_str, str):
            logger.warning('Received chunk with invalid metadata: %s', chunk)
            self.errored_assemblies += 1
            return (True, None)

        buffer = self._store_chunk(message_id=message_id, seq=seq, total=total, payload_str=payload_str)

        # Message is still partially assembled after adding this one, normal, not done yet
        if any(index not in buffer for index in range(total)):
            received_chunks = len(buffer)
            logger.debug('Received chunk %d/%d for message_id=%s, waiting for remainder', received_chunks, total, message_id)
            return (True, None)

        message_str = ''.join(buffer[index] for index in range(total))
        try:
            message_dict = json.loads(message_str)
        except Exception:
            logger.exception(f'Failed to decode chunked message message_id={message_id}')
            self.errored_assemblies += 1
            return (True, None)
        else:
            if not isinstance(message_dict, dict):
                logger.error('Reassembled chunked message is not a dict message_id=%s type=%s', message_id, type(message_dict).__name__)
                self.errored_assemblies += 1
                return (True, None)
        finally:
            self._clear_message_state(message_id)

        self.successful_assemblies += 1
        return (True, message_dict)

    def expire_partial_messages(self, *, current_time: float | None = None) -> None:
        """Drop in-flight messages that have exceeded ``self.message_timeout_seconds`` and log details."""
        if self.message_timeout_seconds <= 0.0:
            return
        if current_time is None:
            current_time = time.monotonic()
        for message_id, started_at in self.assembly_started_at.copy().items():
            age = current_time - started_at
            if age >= self.message_timeout_seconds:
                buffer = self.pending_messages.get(message_id, {})
                received_chunk_indices = sorted(buffer.keys())
                expected_total = self.expected_totals.get(message_id)
                logger.error(
                    (
                        f'Chunked message expired message_id={message_id} '
                        f'age={age:.3f}s timeout={self.message_timeout_seconds:.3f}s '
                        f'received_chunks={received_chunk_indices} expected_total={expected_total} '
                    )
                )
                self.timed_out_assemblies += 1
                self._clear_message_state(message_id)

    async def aexpire_partial_messages(self) -> None:
        """Async wrapper for :meth:`expire_partial_messages`."""
        async with self._lock:
            self.expire_partial_messages()

    async def aget_partial_messages(self) -> dict[str, dict]:
        """Async snapshot of partial assemblies."""
        async with self._lock:
            return self._describe_partials()

    def _clear_message_state(self, message_id: str) -> None:
        """Remove all tracking data for the specified message."""
        self.pending_messages.pop(message_id, None)
        self.expected_totals.pop(message_id, None)
        self.assembly_started_at.pop(message_id, None)

    def _store_chunk(self, *, message_id: str, seq: int, total: int, payload_str: str) -> dict[int, str]:
        """Save chunk data to internal buffers and return the working buffer."""
        buffer = self.pending_messages.setdefault(message_id, {})
        if message_id not in self.assembly_started_at:
            self.assembly_started_at[message_id] = time.monotonic()
        buffer[seq] = payload_str

        existing_total = self.expected_totals.get(message_id)
        if existing_total is not None and existing_total != total:
            logger.warning('Chunk total mismatch for message_id=%s existing=%s new=%s', message_id, existing_total, total)
        self.expected_totals[message_id] = total
        return buffer

    def get_status_data(self) -> dict[str, int]:
        """Return high-level counters for monitoring."""
        return {
            'total_chunks_received': self.total_chunks_received,
            'successful_assemblies': self.successful_assemblies,
            'errored_assemblies': self.errored_assemblies,
            'timed_out_assemblies': self.timed_out_assemblies,
            'active_messages': len(self.pending_messages),
        }

    def _describe_partials(self) -> dict[str, dict]:
        """Return metadata for each in-progress message."""
        snapshot: dict[str, dict] = {}
        current_time = time.monotonic()
        for message_id, buffer in self.pending_messages.items():
            started_at = self.assembly_started_at.get(message_id)
            age = current_time - started_at if started_at is not None else None
            expected_total = self.expected_totals.get(message_id)
            chunk_info = [{'chunk_id': index, 'length': len(payload)} for index, payload in sorted(buffer.items())]
            snapshot[message_id] = {
                'seconds_since_start': age,
                'expected_total': expected_total,
                'chunks': chunk_info,
            }
        return snapshot
