from types import SimpleNamespace

import pytest

from dispatcherd.processors.blocker import Blocker
from dispatcherd.processors.queuer import Queuer
from dispatcherd.service import control_tasks

pytestmark = pytest.mark.asyncio


async def test_metrics_control_without_server():
    dispatcher = SimpleNamespace(metrics=None)

    result = await control_tasks.metrics(dispatcher, {})

    assert result == {'enabled': False}


async def test_metrics_control_with_server():
    class DummyMetrics:
        def get_status_data(self):
            return {'host': 'localhost', 'port': 1, 'ready': True, 'http_server': {'connections_total': 5}}

    dispatcher = SimpleNamespace(metrics=DummyMetrics())

    result = await control_tasks.metrics(dispatcher, {})

    assert result == {'enabled': True, 'status': dispatcher.metrics.get_status_data()}


async def test_cancel_removes_all_matching_blocked_and_queued_tasks():
    queuer = Queuer([])
    blocker = Blocker(queuer)
    queuer.queued_messages.extend({'task': 'lambda: 1', 'uuid': f'queued-{i}'} for i in range(4))
    blocker.blocked_messages.extend({'task': 'lambda: 1', 'uuid': f'blocked-{i}'} for i in range(4))
    other = {'task': 'other', 'uuid': 'other-1'}
    queuer.queued_messages.append(other)
    dispatcher = SimpleNamespace(pool=SimpleNamespace(workers=[], blocker=blocker, queuer=queuer), delayer=[])

    result = await control_tasks._find_tasks(dispatcher, {'task': 'lambda: 1'}, cancel=True)

    assert sorted(msg['uuid'] for msg in result.values()) == [f'{kind}-{i}' for kind in ('blocked', 'queued') for i in range(4)]
    assert blocker.blocked_messages == []
    assert queuer.queued_messages == [other]
