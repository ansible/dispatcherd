import logging
import time

from dispatcherd.config import settings
from dispatcherd.factories import get_broker
from dispatcherd.publish import task

logger = logging.getLogger(__name__)


@task(queue='test_channel')
def sleep_function(seconds=1):
    time.sleep(seconds)


def unregistered_task():
    print('hello from unregistered task')


@task(queue='test_channel', on_duplicate='discard')
def sleep_discard(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='serial')
def sleep_serial(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='queue_one')
def sleep_queue_one(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel')
def print_hello():
    print('hello world!!')


@task(queue='test_channel', bind=True)
def hello_world_binder(binder):
    print(f'Values in binder {vars(binder)}')
    print(f'Hello world, from worker {binder.worker_id} running task {binder.uuid}')


@task(queue='test_channel', timeout=1)
def task_has_timeout():
    time.sleep(5)


def get_queue_name():
    return 'test_channel'


@task(queue=get_queue_name)
def use_callable_queue():
    print('sucessful run using callable queue')


@task(queue=get_queue_name)
class RunJob:
    def run(self):
        print('successful run using callable queue with class')


@task(bind=True)
def prints_running_tasks(binder):
    r = binder.control('running')
    print(f'Obtained data on running tasks, result:\n{r}')


@task(bind=True)
def schedules_another_task(binder):
    r = binder.control('run', data={'task': 'tests.data.methods.print_hello'})
    print(f'Scheduled another task, result: {r}')


_shared_task_broker = None


def _task_broker():
    """The configured broker, cached so tasks in one worker process share its connections.

    On pg_notify sharing comes from the connection_saver factory, but the
    sqlserver broker owns its connections per instance, so the instance
    itself has to live across tasks for the disruption tests to be meaningful.
    """
    global _shared_task_broker
    if _shared_task_broker is None:
        name, config = next(iter(settings.brokers.items()))
        _shared_task_broker = get_broker(name, config)
    return _shared_task_broker


@task()
def break_connection():
    """
    Interact with the database in an intentionally breaking way.

    After this finishes, queries made by this connection are expected to error
    with "the connection is closed"

    This is obviously a problem for any task that comes afterwards.
    So this is used to break things so that the fixes may be demonstrated.
    """
    broker = _task_broker()
    conn = broker.get_connection()

    if broker.__module__ == 'dispatcherd.brokers.sqlserver':
        # No idle session timeout exists on SQL Server, kill the session from the outside instead
        from dispatcherd.brokers.sqlserver import connect as connect_sqlserver

        spid = conn.cursor().execute('SELECT @@SPID').fetchone()[0]
        admin = connect_sqlserver(dict(broker.config))
        try:
            admin.cursor().execute(f'KILL {int(spid)}')
        finally:
            admin.close()
        print(f'sleeping after killing own session, spid={spid}')
        time.sleep(0.2)
    else:
        with conn.cursor() as cursor:
            cursor.execute("SET idle_session_timeout = '0.1s';")

        print('sleeping for 0.2s > 0.1s session timeout')
        time.sleep(0.2)

    print(f'Connection reports closed {getattr(conn, "closed", "not_found")}')

    for i in range(1, 3):
        print(f'\nRunning query number {i}')
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1;")
                print('  query worked, not expected')
            finally:
                cursor.close()
        except Exception as exc:
            print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')

    print(f'Connection reports closed {getattr(conn, "closed", "not_found")}')


def test_break_connection():
    from dispatcherd.config import setup

    setup(file_path='dispatcher.yml')
    break_connection()


@task()
def do_database_query():
    "Just a normal method interacting with the database"
    print('Trying to execute and print result of normal query')
    broker = _task_broker()
    conn = broker.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        logger.info(f"legitimate result of query: {result[0]}")  # prints: 1
        print(f"legitimate result of query: {result[0]}")
