"""Unit tests for dispatcherd.utils.chunking.

Test plan
---------
* split_message
  - handles multi-byte unicode payloads without splitting characters
  - respects max_bytes limit 1) equal to message len 2) smaller than metadata overhead
  - produces deterministic chunk metadata (chunk_index ordering, message_id consistency)
  - raises helpful errors when max_bytes too small
* estimate_wrapper_bytes
  - overhead grows with chunk_index digits
* ChunkAccumulator.ingest_dict
  - returns passthrough for non-chunk payloads
  - assembles multiple chunks in order
  - handles out-of-order chunks
  - drops/raises on version mismatch or missing metadata
  - clears state after completion
"""

import json

import pytest

from dispatcherd.utils import split_message


def test_split_message_handles_unicode_boundaries():
    payload = '{"data":"' + 'é😊漢字' * 30 + '"}'
    max_bytes = 256

    chunks = split_message(payload, max_bytes=max_bytes)

    assert len(chunks) > 1  # should slice due to multi-byte characters

    reconstructed_parts = []
    message_id = None

    for idx, chunk in enumerate(chunks):
        encoded = chunk.encode('utf-8')
        assert len(encoded) <= max_bytes

        chunk_dict = json.loads(chunk)
        if message_id is None:
            message_id = chunk_dict['message_id']
        assert chunk_dict['message_id'] == message_id
        assert chunk_dict['chunk_index'] == idx
        assert chunk_dict['payload']

        reconstructed_parts.append(chunk_dict['payload'])

    assert ''.join(reconstructed_parts) == payload


def test_split_message_handles_escaped_characters_without_backtracking():
    pattern = '\\"quoted\\" segment\\n'
    payload = '{"data":"' + pattern * 50 + '"}'
    max_bytes = 300

    chunks = split_message(payload, max_bytes=max_bytes)

    assert len(chunks) > 1
    reassembled_parts = []
    for chunk in chunks:
        payload_part = json.loads(chunk)['payload']
        assert payload_part
        reassembled_parts.append(payload_part)
    reassembled = ''.join(reassembled_parts)
    assert reassembled == payload


def test_split_message_reaches_escape_limit_when_budget_too_small():
    payload = '{"data":"' + '😊' * 60 + '"}'
    max_bytes = 149  # payload budget smaller than escaped character count

    with pytest.raises(ValueError):
        split_message(payload, max_bytes=max_bytes)


def test_split_boundary_cases():
    "By testing every character count we assure we clip at some point"
    max_bytes = 300
    pattern = 'a'

    for multiplier in range(1, 1000):
        payload = '{"data":"' + (pattern * multiplier) + '"}'
        chunks = split_message(payload, max_bytes=max_bytes)

        first_chunk_dict = json.loads(chunks[0])
        if 'payload' not in first_chunk_dict:
            assert len(chunks) == 1
            assert chunks[0] == payload
            continue

        reconstructed = []
        for chunk in chunks:
            encoded = chunk.encode('utf-8')
            assert len(encoded) <= max_bytes

            chunk_payload = json.loads(chunk)['payload']
            assert chunk_payload
            reconstructed.append(chunk_payload)

        assert ''.join(reconstructed) == payload
