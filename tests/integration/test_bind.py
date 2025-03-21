import asyncio
import json
import logging

import pytest

from tests.data import methods as test_methods

ASSERT_UUID = 'lambda dispatcher: dispatcher.uuid'


@pytest.mark.asyncio
async def test_bind_uuid_matches(apg_dispatcher, pg_message, caplog):
    assert apg_dispatcher.pool.finished_count == 0

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with caplog.at_level(logging.DEBUG):
        await pg_message(json.dumps({'task': ASSERT_UUID, 'uuid': 'hello-world-12345543221', 'bind': True}))
        await asyncio.wait_for(clearing_task, timeout=3)

    assert 'result: hello-world-12345543221' in caplog.text

    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_bind_not_set(apg_dispatcher, pg_message, caplog):
    assert apg_dispatcher.pool.finished_count == 0

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    with caplog.at_level(logging.DEBUG):
        await pg_message(json.dumps({'task': ASSERT_UUID, 'uuid': 'hello-world-12345543221'}))
        await asyncio.wait_for(clearing_task, timeout=3)

    assert 'result: hello-world-12345543221' not in caplog.text

    assert apg_dispatcher.pool.finished_count == 1
