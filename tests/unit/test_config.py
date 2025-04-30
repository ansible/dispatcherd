import json

import pytest
import yaml

from dispatcherd.config import DispatcherSettings, LazySettings, temporary_settings
from dispatcherd.factories import generate_settings_schema


def test_settings_reference_unconfigured():
    settings = LazySettings()
    with pytest.raises(Exception) as exc:
        settings.brokers
    assert 'Dispatcherd not configured' in str(exc)


def test_settings_reference_configured():
    settings = LazySettings()
    settings._wrapped = DispatcherSettings({'version': 2})
    assert settings.brokers == {}
    assert settings.producers == {}
    assert settings.service == {'pool_kwargs': {'max_workers': 3}}
    assert settings.publish == {}
    assert settings.worker == {}
    assert settings.processors == {'queuer_kwargs': {}}


def test_configured_settings():
    settings = LazySettings()
    settings._wrapped = DispatcherSettings({'version': 2, 'brokers': {'pg_notify': {'config': {}}}})
    assert 'pg_notify' in settings.brokers


def test_serialize_settings(test_settings):
    config = test_settings.serialize()
    assert 'producers' in config
    assert 'publish' in config
    assert config['publish'] == {}
    assert 'pg_notify' in config['brokers']
    assert config['service']['pool_kwargs']['max_workers'] == 3

    re_loaded = DispatcherSettings(config)
    assert re_loaded.brokers == test_settings.brokers
    assert re_loaded.service == test_settings.service


def test_version_validated():
    with pytest.raises(RuntimeError) as exc:
        DispatcherSettings({})
    assert 'config version must match this' in str(exc)


def test_schema_is_current():
    with open('dispatcher.yml', 'r') as f:
        file_contents = f.read()
    demo_data = yaml.safe_load(file_contents)
    with temporary_settings(demo_data):
        expect_schema = generate_settings_schema()
    with open('schema.json', 'r') as sch_f:
        schema_contents = sch_f.read()
    schema_data = json.loads(schema_contents)
    assert schema_data == expect_schema


def test_processors_config():
    config = {
        'version': 2,
        'processors': {
            'queuer_kwargs': {
                'worker_selection_strategy': 'longest-idle'
            }
        }
    }
    settings = DispatcherSettings(config)
    assert settings.processors['queuer_kwargs']['worker_selection_strategy'] == 'longest-idle'
