import pytest

from dispatcher.config import DispatcherSettings, LazySettings


def test_settings_reference_unconfigured():
    settings = LazySettings()
    with pytest.raises(Exception) as exc:
        settings.brokers
    assert 'Dispatcher not configured' in str(exc)


def test_configured_settings():
    settings = LazySettings()
    settings._wrapped = DispatcherSettings({'brokers': {'pg_notify': {'config': {}}}})
    'pg_notify' in settings.brokers


def test_serialize_settings(test_settings):
    config = test_settings.serialize()
    assert 'producers' in config
    assert 'publish' in config
    assert config['publish'] == {}
    assert 'pg_notify' in config['brokers']
    assert config['service']['max_workers'] == 3

    re_loaded = DispatcherSettings(config)
    assert re_loaded.brokers == test_settings.brokers
    assert re_loaded.service == test_settings.service
