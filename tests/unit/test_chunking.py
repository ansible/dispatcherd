"""Unit tests for dispatcherd.chunking.

Test plan
---------
* split_message
  - handles multi-byte unicode payloads without splitting characters
  - respects max_bytes limit 1) equal to message len 2) smaller than metadata overhead
  - produces deterministic chunk metadata (index ordering, message_id consistency)
  - raises helpful errors when max_bytes too small
* estimate_wrapper_bytes
  - overhead grows with index digits
* ChunkAccumulator.ingest_dict
  - returns passthrough for non-chunk payloads
  - assembles multiple chunks in order
  - handles out-of-order chunks
  - drops/raises on version mismatch or missing metadata
  - clears state after completion
"""

import json
import logging

import pytest

from dispatcherd.chunking import CHUNK_MARKER, ChunkAccumulator, _wrapper_overhead_bytes, split_message


def test_split_message_handles_unicode_boundaries():
    payload = '{"data":"' + 'é😊漢字' * 30 + '"}'
    max_bytes = 256

    chunks = split_message(payload, max_bytes=max_bytes)

    assert len(chunks) > 1  # should slice due to multi-byte characters

    reconstructed_parts = []
    message_id = None
    total_chunks = None

    for idx, chunk in enumerate(chunks):
        encoded = chunk.encode('utf-8')
        assert len(encoded) <= max_bytes

        chunk_dict = json.loads(chunk)
        if message_id is None:
            message_id = chunk_dict['message_id']
        assert chunk_dict['message_id'] == message_id
        assert chunk_dict['index'] == idx
        if total_chunks is None:
            total_chunks = chunk_dict['total']
        assert chunk_dict['total'] == len(chunks)
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
    max_bytes = 100  # payload budget smaller than escaped character count

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


def test_split_message_respects_exact_size_limits():
    payload = '{"data":"xyz"}'
    max_bytes = len(payload.encode('utf-8'))

    assert split_message(payload, max_bytes=max_bytes) == [payload]


def test_split_message_errors_when_metadata_cannot_fit():
    payload = '{"data":"' + 'x' * 200 + '"}'
    with pytest.raises(ValueError):
        split_message(payload, max_bytes=100)


def test_wrapper_overhead_increases_with_index_digits():
    message_id = 'abcd' * 8
    single_digit = _wrapper_overhead_bytes(message_id, 9, 9)
    double_digit = _wrapper_overhead_bytes(message_id, 10, 10)
    assert double_digit > single_digit


def _make_chunk_dicts(payload: str, *, max_bytes: int) -> list[dict]:
    chunks = split_message(payload, max_bytes=max_bytes)
    return [json.loads(chunk) for chunk in chunks]


def test_chunk_accumulator_passthrough_for_non_chunk_payloads():
    acc = ChunkAccumulator()
    payload = {'data': 'plain'}

    assert acc.ingest_dict(payload) == (False, payload)
    assert acc.total_chunks_received == 0
    assert acc.successful_assemblies == 0
    assert acc.errored_assemblies == 0
    assert acc.timed_out_assemblies == 0


def test_chunk_accumulator_assembles_in_order():
    payload = {'data': 'x' * 200}
    chunk_dicts = _make_chunk_dicts(json.dumps(payload), max_bytes=200)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    for idx, chunk in enumerate(chunk_dicts):
        is_chunk, completed = acc.ingest_dict(chunk)
        assert is_chunk
        if idx < len(chunk_dicts) - 1:
            assert completed is None
        else:
            assert completed == payload
    assert acc.total_chunks_received == len(chunk_dicts)
    assert acc.successful_assemblies == 1
    assert acc.errored_assemblies == 0
    assert acc.timed_out_assemblies == 0


def test_chunk_accumulator_handles_out_of_order_chunks():
    payload = {'data': 'z' * 500}
    chunk_dicts = _make_chunk_dicts(json.dumps(payload), max_bytes=220)
    assert len(chunk_dicts) >= 3

    acc = ChunkAccumulator()
    ingest_order = [1, len(chunk_dicts) - 1] + list(range(2, len(chunk_dicts) - 1)) + [0]

    completed = None
    for index in ingest_order:
        is_chunk, completed = acc.ingest_dict(chunk_dicts[index])
        assert is_chunk
        if index != ingest_order[-1]:
            assert completed is None

    assert completed == payload
    assert acc.total_chunks_received == len(chunk_dicts)
    assert acc.successful_assemblies == 1
    assert acc.errored_assemblies == 0


def test_chunk_accumulator_rejects_missing_metadata():
    payload = {'data': 'oops' * 80}
    chunk = _make_chunk_dicts(json.dumps(payload), max_bytes=200)[0]
    chunk.pop('index', None)

    acc = ChunkAccumulator()
    assert acc.ingest_dict(chunk) == (True, None)
    assert acc.total_chunks_received == 1
    assert acc.errored_assemblies == 1
    assert acc.successful_assemblies == 0


def test_chunk_accumulator_rejects_version_mismatch(caplog):
    payload = {'data': 'x' * 50}
    chunk = _make_chunk_dicts(json.dumps(payload), max_bytes=80)[0]
    chunk[CHUNK_MARKER] = 'dispatcherd.v0'

    acc = ChunkAccumulator()
    with caplog.at_level(logging.ERROR, logger='dispatcherd.chunking'):
        is_chunk, completed = acc.ingest_dict(chunk)
    assert is_chunk
    assert completed is None
    assert 'Unsupported chunk version' in caplog.text
    assert acc.total_chunks_received == 1
    assert acc.errored_assemblies == 1


def test_chunk_accumulator_clears_state_after_completion():
    payload = {'data': 'cleanup' * 40}
    chunk_dicts = _make_chunk_dicts(json.dumps(payload), max_bytes=220)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    for chunk in chunk_dicts:
        acc.ingest_dict(chunk)
    assert acc.pending_messages == {}
    assert acc.expected_totals == {}
    assert acc.assembly_started_at == {}
    assert acc.successful_assemblies == 1
    assert acc.total_chunks_received == len(chunk_dicts)


def test_chunk_accumulator_expires_old_messages(caplog):
    payload = {'data': 'expire_me' * 60}
    chunk_dicts = _make_chunk_dicts(json.dumps(payload), max_bytes=300)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    first_chunk = chunk_dicts[0]
    acc.ingest_dict(first_chunk)
    msg_id = first_chunk['message_id']
    started_at = acc.assembly_started_at[msg_id]

    with caplog.at_level(logging.ERROR, logger='dispatcherd.chunking'):
        acc.expire_partial_messages(current_time=started_at + acc.message_timeout_seconds + 1.0)
    assert any(msg_id in record.getMessage() for record in caplog.records)
    assert msg_id not in acc.pending_messages
    assert msg_id not in acc.expected_totals
    assert msg_id not in acc.assembly_started_at
    assert acc.total_chunks_received == 1
    assert acc.timed_out_assemblies == 1


def test_chunk_accumulator_logs_partial_progress(caplog):
    payload = {'data': 'progress' * 60}
    chunk_dicts = _make_chunk_dicts(json.dumps(payload), max_bytes=250)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    msg_id = chunk_dicts[0]['message_id']
    with caplog.at_level(logging.DEBUG, logger='dispatcherd.chunking'):
        is_chunk, completed = acc.ingest_dict(chunk_dicts[0])
    assert is_chunk
    assert completed is None
    assert any(f'message_id={msg_id}' in record.getMessage() for record in caplog.records)
    assert acc.total_chunks_received == 1
    assert acc.successful_assemblies == 0
    assert acc.errored_assemblies == 0
    assert acc.timed_out_assemblies == 0


def test_chunk_accumulator_handles_non_dict_payload(caplog):
    payload_list = ['invalid'] * 200
    raw = json.dumps(payload_list)
    raw_bytes = len(raw.encode('utf-8'))
    chunk_dicts = _make_chunk_dicts(raw, max_bytes=raw_bytes // 2)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    with caplog.at_level(logging.ERROR, logger='dispatcherd.chunking'):
        final_result = None
        for chunk in chunk_dicts:
            final_result = acc.ingest_dict(chunk)
    assert final_result == (True, None)
    assert 'not a dict' in caplog.text or 'Reassembled chunked message is not a dict' in caplog.text
    assert acc.total_chunks_received == len(chunk_dicts)
    assert acc.errored_assemblies == 1
    assert acc.successful_assemblies == 0


def test_chunk_accumulator_handles_invalid_json(caplog):
    payload = {'data': 'x' * 2000}
    raw = json.dumps(payload)
    raw_bytes = len(raw.encode('utf-8'))
    chunk = _make_chunk_dicts(raw, max_bytes=raw_bytes // 2)[0]
    chunk['payload'] = '{"bad":'  # invalid JSON fragment for assembly
    chunk['total'] = 1
    chunk['index'] = 0

    acc = ChunkAccumulator()
    with caplog.at_level(logging.ERROR, logger='dispatcherd.chunking'):
        result = acc.ingest_dict(chunk)
    assert result == (True, None)
    assert 'Failed to decode chunked message' in caplog.text
    assert acc.total_chunks_received == 1
    assert acc.errored_assemblies == 1
