from unittest import mock

import pytest

from dispatcherd.config import temporary_settings
from dispatcherd.factories import process_manager_from_settings


def test_pass_preload_modules():
    test_config = {'version': 2, 'service': {'process_manager_kwargs': {'preload_modules': ['test.not_real.hazmat']}}}
    with temporary_settings(test_config):
        with mock.patch('dispatcherd.service.process.ForkServerManager.__init__', return_value=None) as mock_init:
            process_manager_from_settings()
            mock_init.assert_called_once_with(preload_modules=['test.not_real.hazmat'], settings=mock.ANY)


@pytest.mark.parametrize('broker_name', ['sqlserver', 'dispatcherd.brokers.sqlserver'])
def test_sqlserver_broker_rejects_fork_process_manager(broker_name):
    test_config = {
        'version': 2,
        'brokers': {broker_name: {'config': {'connection_string': 'not-a-real-connection'}}},
        'service': {'process_manager_cls': 'ProcessManager'},
    }
    with temporary_settings(test_config):
        with pytest.raises(RuntimeError, match='ProcessManager'):
            process_manager_from_settings()


def test_sqlserver_broker_allows_forkserver_process_manager():
    test_config = {
        'version': 2,
        'brokers': {'sqlserver': {'config': {'connection_string': 'not-a-real-connection'}}},
        'service': {'process_manager_cls': 'ForkServerManager'},
    }
    with temporary_settings(test_config):
        with mock.patch('dispatcherd.service.process.ForkServerManager.__init__', return_value=None) as mock_init:
            process_manager_from_settings()
            mock_init.assert_called_once_with(settings=mock.ANY)


def test_pg_broker_allows_fork_process_manager():
    test_config = {
        'version': 2,
        'brokers': {'pg_notify': {'config': {'conninfo': 'dbname=not_a_real_db'}}},
        'service': {'process_manager_cls': 'ProcessManager'},
    }
    with temporary_settings(test_config):
        with mock.patch('dispatcherd.service.process.ProcessManager.__init__', return_value=None) as mock_init:
            process_manager_from_settings()
            mock_init.assert_called_once_with(settings=mock.ANY)
