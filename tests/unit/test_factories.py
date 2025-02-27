from unittest import mock

from dispatcher.factories import process_manager_from_settings
from dispatcher.config import temporary_settings, DispatcherSettings


def test_pass_preload_modules():
    test_config = {
        'version': 2,
        'service': {
            'process_manager_kwargs': {
                'preload_modules': [
                    'test.not_real.hazmat'
                ]
            }
        }
    }
    with temporary_settings(test_config):
        with mock.patch('dispatcher.service.process.ForkServerManager.__init__', return_value=None) as mock_init:
            process_manager_from_settings()
            mock_init.assert_called_once_with(preload_modules=['test.not_real.hazmat'])
