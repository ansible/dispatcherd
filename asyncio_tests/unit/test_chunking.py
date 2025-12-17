import json

import pytest

from dispatcherd.chunking import ChunkAccumulator
from tests.unit import test_chunking as sync_chunking


@pytest.mark.asyncio
async def test_chunk_accumulator_reports_partial_messages():
    payload = {'data': 'monitor' * 30}
    chunk_dicts = sync_chunking._make_chunk_dicts(json.dumps(payload), max_bytes=200)
    assert len(chunk_dicts) > 1

    acc = ChunkAccumulator()
    acc.ingest_dict(chunk_dicts[0])

    snapshot = await acc.aget_partial_messages()
    assert chunk_dicts[0]['message_id'] in snapshot
    details = snapshot[chunk_dicts[0]['message_id']]
    assert details['expected_total'] == len(chunk_dicts)
    assert details['seconds_since_start'] is not None
    assert details['chunks'][0]['chunk_id'] == chunk_dicts[0]['index']
    assert details['chunks'][0]['length'] == len(chunk_dicts[0]['payload'])
