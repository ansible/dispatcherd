"""Application examples for Django + mssql-django's pyodbc driver.

Copy these helpers into your application; Django is not a dispatcherd dependency.
Broker instances created here are scoped to the current synchronous call.
"""

import pyodbc  # type: ignore[import-not-found,import-untyped]
from django.db import connections, transaction  # type: ignore[import-not-found,import-untyped]

from dispatcherd.brokers.sqlserver import Broker


def django_sqlserver_connection(*, using: str = 'default', **config) -> pyodbc.Connection:
    """A sync_connection_factory for a short-lived broker in the current thread.

    Extra broker connection options are intentionally ignored: Django owns this
    connection's credentials, autocommit, timeout and transaction configuration.
    """
    database = connections[using]
    database.validate_thread_sharing()
    database.ensure_connection()
    database.validate_no_broken_transaction()
    if database.vendor != 'microsoft' or not isinstance(database.connection, pyodbc.Connection):
        raise TypeError('Use mssql-django with its pyodbc driver for borrowed SQL Server publishing')
    return database.connection


def publish_in_transaction(channel: str, message: str, *, using: str = 'default') -> list[str]:
    """Broadcast on Django's connection, joining the caller's atomic transaction.

    Without an outer atomic block this creates a transaction for the publication.
    Errors mark an outer transaction for rollback even if the caller catches them.
    """
    database = connections[using]
    with transaction.atomic(using=using, savepoint=False):
        connection = django_sqlserver_connection(using=using)
        broker = Broker(config={}, sync_connection=connection)
        try:
            # Raw ODBC bypasses Django's cursor wrapper. Translate driver errors
            # here; the surrounding atomic block handles rollback state.
            with database.wrap_database_errors:
                return broker.broadcast(channel, message)
        finally:
            broker.close()  # The borrowed Django connection remains open.
