import logging

import psycopg

logger = logging.getLogger(__name__)


"""This module exists under the theory that dispatcher messaging should be swappable

to different message busses eventually.
That means that the main code should never import psycopg.
Thus, all psycopg-lib-specific actions must happen here.
"""


# TODO: get database data from settings
# # As Django settings, may not use
# DATABASES = {
#     "default": {
#         "ENGINE": "django.db.backends.postgresql",
#         "HOST": os.getenv("DB_HOST", "127.0.0.1"),
#         "PORT": os.getenv("DB_PORT", 55777),
#         "USER": os.getenv("DB_USER", "dispatch"),
#         "PASSWORD": os.getenv("DB_PASSWORD", "dispatching"),
#         "NAME": os.getenv("DB_NAME", "dispatch_db"),
#     }
# }


async def aget_connection(config):
    return await psycopg.AsyncConnection.connect(**config, autocommit=True)


def get_connection(config):
    return psycopg.Connection.connect(**config, autocommit=True)


async def aprocess_notify(connection, channels, connected_event=None):
    async with connection.cursor() as cur:
        for channel in channels:
            await cur.execute(f"LISTEN {channel};")
            logger.info(f"Set up pg_notify listening on channel '{channel}'")

        if connected_event:
            connected_event.set()

        while True:
            logger.debug('Starting listening for pg_notify notifications')
            async for notify in connection.notifies():
                yield notify.channel, notify.payload


async def apublish_message(connection, channel, payload=None):
    async with connection.cursor() as cur:
        if not payload:
            await cur.execute(f'NOTIFY {channel};')
        else:
            await cur.execute(f"NOTIFY {channel}, '{payload}';")


def get_django_connection():
    try:
        from django.conf import ImproperlyConfigured
        from django.db import connection as pg_connection
    except ImportError:
        return None
    else:
        try:
            if pg_connection.connection is None:
                pg_connection.connect()
            if pg_connection.connection is None:
                raise RuntimeError('Unexpectedly could not connect to postgres for pg_notify actions')
            return pg_connection.connection
        except ImproperlyConfigured:
            return None


def publish_message(queue, message, config=None, connection=None, new_connection=False):
    conn = None
    if connection:
        conn = connection

    if (not conn) and (not new_connection):
        conn = get_django_connection()

    created_new_conn = False
    if not conn:
        if config is None:
            raise RuntimeError('Could not use Django connection, and no postgres config supplied')
        conn = get_connection(config)
        created_new_conn = True

    with conn.cursor() as cur:
        cur.execute('SELECT pg_notify(%s, %s);', (queue, message))

    logger.debug(f'Sent pg_notify message to {queue}')

    if created_new_conn:
        conn.close()
