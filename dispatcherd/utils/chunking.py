"""Message chunking utilities shared across dispatcherd components.

Typical usage is a two-step process:

1. A producer (e.g., a broker implementation) instantiates :class:`MessageChunker`
   and calls :meth:`MessageChunker.split` on JSON strings before sending them.
   Each returned chunk is itself a valid JSON document that includes metadata
   describing the parent message.
2. A consumer (e.g., :class:`dispatcherd.service.main.DispatcherMain`) creates a
   single :class:`ChunkAccumulator` instance and feeds every decoded JSON dict to
   :meth:`ChunkAccumulator.ingest_dict`.  Once all chunks for a message arrive,
   the accumulator returns the fully reconstructed message dict.
"""

import json
import logging
import uuid
from typing import Dict, Optional

logger = logging.getLogger(__name__)

CHUNK_MARKER = '__dispatcherd_chunk__'
CHUNK_VERSION = 'dispatcherd.v1'
DEFAULT_HEADER_RESERVE = 256


class MessageChunker:
    """Producer-side helper for splitting large payloads into chunk envelopes.

    Instantiate with the desired maximum size (in bytes) and call :meth:`split`
    with a JSON string.  The method returns one or more JSON strings whose size
    does not exceed the limit.  Each chunk contains metadata such as the message
    id, chunk index, and a flag indicating the final chunk, allowing the
    consumer to reassemble the original payload.
    """

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
        """Split ``message`` into JSON chunks that respect ``max_bytes``."""
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
    def parse_chunk_dict(candidate: dict) -> Optional[dict]:
        """Return the candidate dict when it matches the chunk envelope schema."""
        if not isinstance(candidate, dict):
            return None
        if candidate.get(CHUNK_MARKER) != CHUNK_VERSION:
            return None
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

    def __init__(self) -> None:
        self.pending_messages: Dict[str, Dict[int, str]] = {}
        self.final_indexes: Dict[str, int] = {}

    def ingest_dict(self, payload_dict: dict) -> tuple[bool, Optional[dict], Optional[str]]:
        """Process a decoded payload dict and assemble chunked messages."""
        chunk = MessageChunker.parse_chunk_dict(payload_dict)
        if not chunk:
            return (False, payload_dict, None)

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

        message_str = ''.join(buffer[index] for index in range(final_seq + 1))
        try:
            message_dict = json.loads(message_str)
            if not isinstance(message_dict, dict):
                raise ValueError('assembled payload is not a dict')
        except Exception:
            logger.exception(f'Failed to decode chunked message message_id={message_id}')
            self.pending_messages.pop(message_id, None)
            self.final_indexes.pop(message_id, None)
            return (True, None, message_id)

        self.pending_messages.pop(message_id, None)
        self.final_indexes.pop(message_id, None)
        return (True, message_dict, message_id)

    def clear(self) -> None:
        """Reset all tracking data, dropping any inflight messages."""
        self.pending_messages.clear()
        self.final_indexes.clear()
