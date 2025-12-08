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


def _wrapper_overhead_bytes(message_id: str, chunk_index: int, total_chunks: int) -> int:
    """Estimate bytes added by chunk metadata for the given index."""
    empty_chunk = _serialize_chunk(message_id, chunk_index, total_chunks, '')
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
    if candidate.get(CHUNK_MARKER) != CHUNK_VERSION:
        raise ValueError(f'Unsupported chunk version: {candidate.get(CHUNK_MARKER)}')
    return candidate


class ChunkAccumulator:
    """Consumer-side helper for reassembling message chunks.

    Create one accumulator per dispatcher (or per consumer) and feed every
    decoded JSON dict to :meth:`ingest_dict`.  The method returns a tuple:

    ``(is_chunk, completed_message, message_id)``

    * ``is_chunk`` indicates whether the payload was part of the chunking
      protocol.
    * ``completed_message`` is the reconstructed dict when the final chunk has
      been seen; otherwise it is ``None``.
    * ``message_id`` allows callers to reference partial state for logging.
    """

    def __init__(self, *, message_timeout_seconds: float = 30 * 60, cleanup_interval_seconds: float | None = None) -> None:
        if message_timeout_seconds <= 0:
            raise ValueError('message_timeout_seconds must be positive')
        if cleanup_interval_seconds is None:
            cleanup_interval_seconds = min(60.0, max(5.0, message_timeout_seconds / 10))

        self.message_timeout_seconds = message_timeout_seconds
        self.cleanup_interval_seconds = max(0.01, cleanup_interval_seconds)
        self.pending_messages: Dict[str, Dict[int, str]] = {}
        self.expected_totals: Dict[str, int] = {}
        self.message_started_at: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def aingest_dict(self, payload_dict: dict) -> tuple[bool, Optional[dict], Optional[str]]:
        """Async wrapper that serializes :meth:`ingest_dict` mutations."""
        async with self._lock:
            return self.ingest_dict(payload_dict)

    def ingest_dict(self, payload_dict: dict) -> tuple[bool, Optional[dict], Optional[str]]:
        """Process a decoded payload dict and assemble chunked messages.

        Scenarios
        ---------
        1. Payload is not chunked: returns ``(False, payload_dict, None)`` so callers
           can process it immediately.
        2. Chunk received but more pieces pending: returns ``(True, None, message_id)``
           allowing the caller to track which message is mid-flight.
        3. Final chunk completes the message: returns ``(True, completed_dict, message_id)``
           with the assembled payload ready for processing.
        4. Chunk metadata missing/invalid: returns ``(True, None, None)`` to signal the
           caller that the chunk could not be associated with a message.
        5. Reassembly fails JSON validation: returns ``(True, None, message_id)`` so the
           caller can log/handle the failure for that specific message.
        """
        chunk = parse_chunk_dict(payload_dict)
        if not chunk:
            return (False, payload_dict, None)

        message_id = chunk.get('message_id') or chunk.get('id')
        seq = chunk.get('index')
        if seq is None:
            seq = chunk.get('chunk_index')
        total = chunk.get('total')
        if total is not None:
            if not isinstance(total, int) or total <= 0:
                logger.warning('Received chunk with invalid total: %s', chunk)
                return (True, None, None)
        is_final = chunk.get('final_chunk')
        if is_final is None:
            is_final = chunk.get('final')

        if not isinstance(message_id, str) or not isinstance(seq, int):
            logger.warning('Received chunk with invalid metadata: %s', chunk)
            return (True, None, None)

        payload_str = chunk.get('payload', '')
        if not isinstance(payload_str, str):
            payload_str = str(payload_str)

        buffer = self.pending_messages.setdefault(message_id, {})
        if message_id not in self.message_started_at:
            self.message_started_at[message_id] = time.monotonic()
        buffer[seq] = payload_str

        if total is not None:
            existing_total = self.expected_totals.get(message_id)
            if existing_total is not None and existing_total != total:
                logger.warning('Chunk total mismatch for message_id=%s existing=%s new=%s', message_id, existing_total, total)
            self.expected_totals[message_id] = total

        if total is None and isinstance(is_final, bool) and is_final:
            self.expected_totals[message_id] = seq + 1

        expected_total = self.expected_totals.get(message_id)
        if expected_total is None:
            return (True, None, message_id)

        if any(index not in buffer for index in range(expected_total)):
            return (True, None, message_id)

        message_str = ''.join(buffer[index] for index in range(expected_total))
        try:
            message_dict = json.loads(message_str)
            if not isinstance(message_dict, dict):
                raise ValueError('assembled payload is not a dict')
        except Exception:
            logger.exception(f'Failed to decode chunked message message_id={message_id}')
            return (True, None, message_id)
        finally:
            self.pending_messages.pop(message_id, None)
            self.expected_totals.pop(message_id, None)
            self.message_started_at.pop(message_id, None)

        return (True, message_dict, message_id)

    def expire_partial_messages(self, *, current_time: float | None = None) -> list[str]:
        """Drop in-flight messages that have exceeded ``self.message_timeout_seconds``."""
        if current_time is None:
            current_time = time.monotonic()
        expired: list[str] = []
        for message_id, started_at in list(self.message_started_at.items()):
            if (current_time - started_at) >= self.message_timeout_seconds:
                expired.append(message_id)
                self.pending_messages.pop(message_id, None)
                self.expected_totals.pop(message_id, None)
                self.message_started_at.pop(message_id, None)
        return expired

    async def aexpire_partial_messages(self) -> list[str]:
        """Async wrapper for :meth:`expire_partial_messages`."""
        async with self._lock:
            return self.expire_partial_messages()

    def get_progress(self, message_id: str) -> tuple[int, Optional[int]]:
        """Return (received_chunks, expected_total) for logging/debugging."""
        buffer = self.pending_messages.get(message_id, {})
        expected_total = self.expected_totals.get(message_id)
        return (len(buffer), expected_total)
