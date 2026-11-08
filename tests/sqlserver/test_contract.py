"""Built-in discovery and distribution tests; no SQL Server connection required."""

from importlib.resources import files

import pytest

from dispatcherd import factories
from dispatcherd.brokers import get_broker
from dispatcherd.config import DispatcherSettings

pytest.importorskip('pyodbc', exc_type=ImportError)
pytest.importorskip('aioodbc')
from dispatcherd.brokers.sqlserver import Broker  # noqa: E402 - import after optional dependency checks


@pytest.mark.parametrize('name', ['sqlserver', 'dispatcherd.brokers.sqlserver'])
def test_builtin_factory_without_connecting(name):
    broker = get_broker(name, {'config': {'connection_string': 'not a real connection'}, 'channels': ['jobs']})
    assert isinstance(broker, Broker)
    assert broker.get_publish_channel() == 'jobs'
    assert broker.instance_id is None
    broker.close()


def test_application_factories():
    settings = DispatcherSettings(
        {
            'version': 2,
            'brokers': {
                'sqlserver': {
                    'config': {'connection_string': 'not a real connection'},
                    'channels': ['app_jobs', 'app_control'],
                    'default_publish_channel': 'app_jobs',
                }
            },
            'service': {'pool_kwargs': {'max_workers': 4}},
        }
    )
    publisher = factories.get_publisher_from_settings(settings=settings)
    control = factories.get_control_from_settings(settings=settings, default_publish_channel='app_control')
    dispatcher = factories.from_settings(settings=settings)
    assert publisher.get_publish_channel() == 'app_jobs'
    assert control.broker_name == 'sqlserver'
    assert control.broker_config['default_publish_channel'] == 'app_control'
    assert isinstance(dispatcher.producers[0].broker, Broker)
    assert dispatcher.producers[0].broker.channels == ('app_jobs', 'app_control')
    assert dispatcher.pool.max_workers == 4
    publisher.close()


def test_configuration_schema_annotations():
    schema = factories.schema_for_cls(Broker)
    assert schema['config'] == "<class 'dict'>"
    assert schema['wait_timeout_ms'] == "<class 'int'>"
    assert 'channels' in schema
    assert 'default_publish_channel' in schema


def test_provisioning_resource_in_package():
    sql = files('dispatcherd.brokers').joinpath('sqlserver_schema.sql').read_text()
    assert 'CREATE OR ALTER PROCEDURE dbo.DispatcherdRegister' in sql
    assert 'CREATE QUEUE dbo.DispatcherdInbox' in sql
