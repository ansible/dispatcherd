from unittest import mock

import pytest

from dispatcher.publish import task
from dispatcher.config import temporary_settings


def test_apply_async_with_no_queue(registry, conn_config):
    @task(registry=registry)
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)

    # These settings do not specify a default channel, that is the main point
    with temporary_settings({'brokers': {'pg_notify': {'config': conn_config}}}):

        # Can not run a method if we do not have a queue
        with pytest.raises(ValueError):
            dmethod.apply_async()

        # But providing a queue at time of submission works
        with mock.patch('dispatcher.brokers.pg_notify.SyncBroker.publish_message') as mock_publish_method:
            dmethod.apply_async(queue='fooqueue')
        mock_publish_method.assert_called_once_with(channel='fooqueue', message=mock.ANY)

    mock_publish_method.assert_called_once()
