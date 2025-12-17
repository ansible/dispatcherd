from types import SimpleNamespace

import pytest

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
