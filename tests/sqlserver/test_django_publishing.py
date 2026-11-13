"""Exercise the application example with real Django transactions and SQL Server."""

import json
import os
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip('django')
pytest.importorskip('mssql')
pytest.importorskip('pyodbc', exc_type=ImportError)
from django.conf import settings  # noqa: E402 - import after optional dependency checks
from django.db import DatabaseError, connections, models, transaction  # noqa: E402 - import after optional dependency checks

# Docker copies the example next to these tests; source checkouts use tools/.
example_dir = Path(__file__).resolve().parents[2] / 'tools' / 'sqlserver'
if example_dir.is_dir():
    sys.path.insert(0, str(example_dir))
from django_publish import django_sqlserver_connection, publish_in_transaction  # noqa: E402 - import after optional dependency checks


@pytest.fixture
def django_record(config):
    import django

    if not settings.configured:
        settings.configure(
            DATABASES={
                'default': {
                    'ENGINE': 'mssql',
                    'NAME': 'dispatcherd',
                    'USER': 'sa',
                    'PASSWORD': os.environ['MSSQL_SA_PASSWORD'],
                    'HOST': os.environ.get('MSSQL_HOST', 'localhost'),
                    'PORT': os.environ.get('MSSQL_PORT', '14333'),
                    'OPTIONS': {'driver': 'ODBC Driver 18 for SQL Server', 'extra_params': 'Encrypt=yes;TrustServerCertificate=yes'},
                },
            },
            INSTALLED_APPS=[],
        )
        django.setup()

    model_namespace = 'dispatcherd_sqlserver_' + uuid.uuid4().hex

    class Record(models.Model):
        value = models.CharField(max_length=32)

        class Meta:
            app_label = model_namespace
            db_table = 'django_publish_' + uuid.uuid4().hex

    database = connections['default']
    with database.schema_editor() as editor:
        editor.create_model(Record)
    try:
        yield Record
    finally:
        with database.schema_editor() as editor:
            editor.delete_model(Record)
        connections.close_all()


@pytest.mark.parametrize('commit', [True, False])
def test_django_atomic_controls_orm_write_and_message(config, brokers, django_record, commit):
    channel = 'django_' + uuid.uuid4().hex
    listener = brokers([channel])
    listener.register()
    message = json.dumps({'task': 'example.task'})
    with transaction.atomic():
        django_record.objects.create(value='pending')
        assert publish_in_transaction(channel, message) == [listener.instance_id]
        assert listener._receive(100) is None
        assert django_record.objects.count() == 1
        if not commit:
            transaction.set_rollback(True)
    assert django_record.objects.count() == int(commit)
    assert listener._receive(100) == ((channel, message) if commit else None)
    assert connections['default'].is_usable()


def test_django_publish_error_marks_outer_transaction_for_rollback(brokers, django_record, monkeypatch):
    from django_publish import Broker

    from dispatcherd.brokers.sqlserver import _rows

    def fail(self, *args, **kwargs):
        _rows(self._get_publisher(), "SET XACT_ABORT ON; THROW 50000, 'SQL publishing failure', 1;")

    monkeypatch.setattr(Broker, 'broadcast', fail)
    with transaction.atomic():
        django_record.objects.create(value='must roll back')
        with pytest.raises(DatabaseError, match='publishing failure'):
            publish_in_transaction('jobs', '{}')
        assert transaction.get_rollback()
    assert django_record.objects.count() == 0


def test_django_factory_uses_reopened_connection(django_record):
    first = django_sqlserver_connection()
    connections['default'].close()
    second = django_sqlserver_connection()
    assert first is not second
    assert connections['default'].is_usable()
