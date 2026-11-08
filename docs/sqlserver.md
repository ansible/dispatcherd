# Using SQL Server with dispatcherd

The built-in `sqlserver` [dispatcherd broker](config.md#brokers) lets your
application send tasks and control requests through a SQL Server database. This
guide explains how SQL Server carries those messages, then walks through database
setup and application configuration. No prior knowledge of SQL Server messaging
is needed.

## SQL Server concepts used here

SQL Server has a messaging system inside the database engine. These are the parts
that dispatcherd uses; setup creates the database objects for you.

- **[Service Broker](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-service-broker)**
  is SQL Server's built-in messaging feature. Applications send and receive
  messages using SQL commands on database connections.
- **Queue** is a database object holding messages until an application receives
  them. Receiving removes messages from the queue when the transaction commits.
- **Service** is a named SQL object that directs messages to a queue. A SQL service
  is separate from a running dispatcherd process; many processes use the same
  fixed SQL services.
- **Conversation**, also called a **dialog**, connects two SQL services for message
  exchange. It can carry multiple messages before it is closed. SQL Server assigns
  it a **conversation ID**, shared by both sides. dispatcherd uses this ID to
  identify the listening instance for that conversation.
- **Conversation endpoint** is SQL Server's database record for one side of a
  conversation. Each conversation has two endpoints, which track each side's
  state. The side that starts the conversation is the **initiator**; the other
  is the **target**. Both endpoint records live in the same database in this
  backend.
- **Conversation handle** is the UUID identifying an endpoint. Code supplies
  this handle when sending, receiving or closing that side of the conversation.
  A message sent using one endpoint's handle is received using the other
  endpoint's handle.
- **[Message type and contract](https://learn.microsoft.com/en-us/sql/database-engine/service-broker/contracts?view=sql-server-ver17)**
  describe which messages a conversation permits and which side can send them.
  dispatcherd defines a message type for its payloads and separate contracts for
  service notifications and temporary replies. That separation also lets cleanup
  distinguish the two kinds of conversation.

All of these objects live in one database for this backend. You do not need to
configure messaging between SQL Server hosts.

## How messages reach listening instances

If you have used dispatcherd with PostgreSQL `LISTEN`/`NOTIFY`, you may expect
publishing to a channel to notify every listening service. This backend preserves
that behavior, but SQL Server queues require a different implementation. Having
several processes receive from one queue would divide the messages among them;
it would not give each process its own copy.

dispatcherd gives each listening instance a conversation. The conversation ID is
also the instance ID; there is no second generated identity. Each instance receives
only its conversation's messages from the shared queue.

Channel membership is a database relationship: `DispatcherdSubscription` contains
one row per `(channel, conversation_id)`. To find the instances listening on a
channel named `broadcast_all`, a publisher can query:

```sql
SELECT conversation_id
FROM dbo.DispatcherdSubscription
WHERE channel = N'broadcast_all';
```

The backend resolves these conversations' sending handles through SQL Server's
endpoint catalog. A small parent table, `DispatcherdListenerRegistry`, holds only
the conversation ID, optional node label and registration timestamp. It also
provides a row lock to coordinate sending with removal of a registration. Neither
table stores endpoint handles or readiness state.

These relationships support three delivery choices:

- **Broadcast (fan-out):** send a copy to every registered instance subscribed to
  the channel. This is the default for `submit_task`, `publish_message` and
  `apublish_message`. The backend looks up subscribers and sends on each one's
  conversation. Two services listening on `app_jobs` can therefore both run the
  same submitted task.
- **Anycast:** choose one subscribed instance at random and send only on its
  conversation. Use `send_any` or `asend_any` when you want one service to receive
  the message. This does not choose based on load or retry another instance if
  the selected one has stopped responding.
- **Targeted send:** select a specific instance by its conversation ID. Use
  `send_to` or `asend_to`; the backend verifies that the instance is still
  registered and subscribed to the requested channel before sending.

A service's subprocess pool shares the messages received by its main process.
The backend does not register each subprocess separately. Channel names are
subscription data in the tables; adding a channel does not create another SQL queue.

Start listeners before publishing. Although SQL Server stores queued messages,
this backend provides best-effort notifications: a successful send does not prove
that a task ran, and dispatcherd does not reassign messages from a crashed service.
Removing a registration discards its pending messages. Restarting or recycling
a listener creates a new conversation ID, without replaying the old instance's
work. Registrations and messages are disposable runtime data; installing a changed
schema may discard all of them.

## Set up your application

### 1. Install the database driver and Python packages

Python connects to SQL Server through ODBC, a database-driver interface. This
backend uses Microsoft's native ODBC driver underneath two Python libraries:
`pyodbc` for synchronous calls and `aioodbc` for async calls.

Install the Python packages in your application's virtual environment:

```sh
python3 -m venv .venv
.venv/bin/pip install 'dispatcherd[sqlserver]'
# For this source checkout instead:
# .venv/bin/pip install -e '.[sqlserver]'
```

Install unixODBC and Microsoft's ODBC Driver 18 on Linux, or the corresponding
Microsoft driver on your platform. The `sqlserver` extra installs both Python
libraries; install the native driver separately. See the
[Microsoft driver installation instructions](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
All service and publishing processes need the extra and driver. Installing the
base `dispatcherd` package does not load or require the SQL Server driver.

The integration tests use SQL Server 2022 Developer on Linux. Choose a SQL Server
host that supports Service Broker; Azure SQL Database does not provide this
feature. All dispatcherd services and publishers must connect to the same database.
For a local database, see [the Docker instructions below](#run-the-integration-tests).

### 2. Create the database and install dispatcherd's SQL objects

Create or choose a database for dispatcherd messages and listener registrations.
The examples below use `dispatcherd` as the database name. For publishing inside
a Django transaction, install the SQL objects in the same database used by that
transaction; see [the Django examples](#publish-from-django).
Verify that its messaging feature is enabled:

```sql
SELECT name, is_broker_enabled FROM sys.databases WHERE name = 'dispatcherd';
-- If disabled, schedule this administrative operation when exclusive access is available:
-- ALTER DATABASE dispatcherd SET ENABLE_BROKER;
```

Next, run the setup method with credentials allowed to create database objects.
Do this before starting dispatcherd:

```python
import os
from dispatcherd.brokers.sqlserver import Broker

Broker.setup_database({
    'connection_string': os.environ['DISPATCHERD_SQLSERVER_ADMIN_CONNECTION_STRING'],
})
```

The connection string must select `dispatcherd` (the format is shown in the
next step). Setup creates the queues, SQL services, message type and contracts
introduced above, along with the registration tables and stored procedures used
to send, receive and clean up messages. It also creates a database role named
`dispatcherd_runtime` for application access.

You can rerun setup against the same schema. Schema changes require a fresh
database or removal and recreation of the backend's objects; there are no data
migrations. Setup reports an error if Service Broker is disabled. Enabling that feature remains an
administrative step because it can require exclusive access to the database.
Starting dispatcherd or publishing a task never runs setup automatically.

Grant the application's database user membership in the runtime role:

```sql
USE dispatcherd;
-- If needed, map a login that your DBA has already created:
CREATE USER dispatcherd_app FOR LOGIN dispatcherd_app;
ALTER ROLE dispatcherd_runtime ADD MEMBER dispatcherd_app;
```

The stored procedures run with their owner's permissions, so application users do
not need permission to create or alter database objects. Keep setup credentials
separate from application credentials. Members of `dispatcherd_runtime` can send
to and remove any listener registration in this database; give this role to trusted
application processes.

### 3. Configure dispatcherd in services and publishers

Supply the runtime connection string through your application's secret/configuration
system. For example, the environment variable below could contain:

```text
DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql.example.net,1433;DATABASE=dispatcherd;UID=dispatcherd_app;PWD=<secret>;Encrypt=yes;TrustServerCertificate=no;
```

Use a server certificate trusted by the application host. Development containers
may use `TrustServerCertificate=yes`, as in the local YAML example below. Escape
ODBC values appropriately when constructing a connection string, particularly
passwords containing semicolons or closing braces.

Create an application configuration module, such as `my_app/dispatcher_config.py`:

```python
import os
from dispatcherd.config import setup

CONFIG = {
    'version': 2,
    'brokers': {
        'sqlserver': {
            'config': {
                'connection_string': os.environ['DISPATCHERD_SQLSERVER_CONNECTION_STRING'],
            },
            'channels': ['app_jobs', 'app_control'],
            'default_publish_channel': 'app_jobs',
            'wait_timeout_ms': 1000,
        },
    },
    'service': {
        'pool_kwargs': {'max_workers': 4},
    },
}

def configure():
    return setup(CONFIG)
```

Call `configure()` in both the service entry point and each publishing process
before using dispatcherd. Application task modules must be importable in both.
The `channels` list makes each service listen for both jobs and control requests.
`default_publish_channel` sends ordinary publications to `app_jobs` unless the
caller supplies another channel. Each service creates its own registration when
it starts listening. Its subprocess workers share that registration.

Control replies identify the service using `DispatcherMain.node_id`, which is
unique by default. Set `service.main_kwargs.node_id` if you want a descriptive
name. Directly targeting a service uses its SQL Server conversation ID, which
changes on restart or recycling.

For a file-based deployment, the equivalent `dispatcher.yml` is:

```yaml
version: 2
brokers:
  sqlserver:
    config:
      connection_string: 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,14333;DATABASE=dispatcherd;UID=dispatcherd_app;PWD=<runtime-secret>;Encrypt=yes;TrustServerCertificate=yes;'
    channels: [app_jobs, app_control]
    default_publish_channel: app_jobs
    wait_timeout_ms: 1000
service:
  pool_kwargs:
    max_workers: 4
```

Render that file securely through your application's deployment system. The YAML
loader does **not** interpolate environment variable placeholders. Do not commit
credentials. Run `.venv/bin/dispatcherd --config /path/to/dispatcher.yml`, and use
`setup(file_path=...)` or `DISPATCHERD_CONFIG_FILE` in publishers.

### 4. Start a listener, then publish a task

A Python service entry point can use the configuration module directly:

```python
from dispatcherd import run_service
from my_app.dispatcher_config import configure

if __name__ == '__main__':
    configure()
    run_service()
```

Define an importable task in `my_app/tasks.py` using the existing decorator:

```python
from dispatcherd.publish import task

@task(queue='app_jobs', decorate=False)
def rebuild_index():
    print('Rebuilding the application index')
```

In another process, configure dispatcherd and submit that task:

```python
from dispatcherd.publish import submit_task
from my_app.dispatcher_config import configure
from my_app.tasks import rebuild_index

configure()
submit_task(rebuild_index, queue='app_jobs')
```

## Select one recipient or inspect subscribers

Ordinary task submission above broadcasts to the channel. For an explicit delivery
choice, get the configured broker and send a task message directly. The following
example chooses one subscriber:

```python
import json
from dispatcherd.factories import get_publisher_from_settings
from my_app.dispatcher_config import configure

configure()
broker = get_publisher_from_settings()  # this configuration has one broker
message = json.dumps({'task': 'my_app.tasks.rebuild_index'})
try:
    selected_id = broker.send_any('app_jobs', message)
finally:
    broker.close()
```

`selected_id` is the chosen conversation ID, or `None` if no send succeeded.
Other operations on the same broker are:

- `broker.snapshot('app_jobs')` lists registrations subscribed to the channel.
  Each result includes `instance_id`, whose value is the conversation ID accepted
  by `send_to`. Endpoint handles in the snapshot are read from SQL Server's catalog.
- `broker.send_to(instance_id, 'app_jobs', message)` sends to that registration.
  It returns `False` if the registration is absent, retired or unsubscribed.
- `broker.broadcast('app_jobs', message)` sends to all subscribers and returns
  the conversation IDs it successfully sent to.

These return values describe sends, not acknowledgments from the receiving
processes. Registrations can outlive the processes that created them.

In async code, use `asnapshot`, `asend_any`, `asend_to` or `abroadcast`, and close
with `await broker.aclose()`. Task message contents follow the existing
[message format](message_formats.md); the backend handles splitting large messages
into chunks and reassembling them through dispatcherd's existing chunking support.

## Send a Control request and collect replies

The example configuration gives control requests their own `app_control` channel.
Send an `alive` request to the services listening there:

```python
from dispatcherd.factories import get_control_from_settings
from my_app.dispatcher_config import configure

configure()
control = get_control_from_settings(default_publish_channel='app_control')
replies = control.control_with_reply('alive', expected_replies=3, timeout=5)
# In an async application:
# replies = await control.acontrol_with_reply('alive', expected_replies=3, timeout=5)
```

Set `expected_replies` to the number of service instances you expect. The request
is broadcast, and replies identify services by their `node_id` values.

The calling application needs somewhere to receive these replies, even though it
is not a dispatcherd service. Before sending the request, the backend creates a
temporary conversation and puts a return address in the request's `reply_to`
field. That address is a single string, `sqlserver-reply:<sending-handle>`.
Applications should let Control create and use it.

Each responding service sends directly using that handle. It does not look up the
caller in the registration or subscription tables, and the caller never registers
as a service. The caller receives on the other endpoint of the same conversation.
Multiple services can reply to one address; simultaneous Control calls each get
their own conversation. The running service uses aioodbc for these replies, just
as it does for its other async database operations.

Choose a timeout greater than zero and less than 300 seconds. It includes the time
spent preparing the reply conversation. The database also gives the conversation
a hard five-minute lifetime. Completion, timeout or cancellation closes both
endpoints and discards leftover replies. Replies sent after the address has closed
or expired are discarded. If the caller crashes before it can clean up, use the
expired-reply cleanup described below.

## Detect connection failures and recover

Each listener sends an initial self-check message through its own conversation
and verifies receipt before calling the service's connected callback. This checks
the database communication path. Publishers can already send as soon as registration
commits; they do not wait for the self-check or query readiness state.

While listening, the backend sends another self-check after a period without
incoming messages. By default that interval is 30 seconds, and the check must
arrive within 2 seconds. It tracks the outstanding check and its deadline in memory.
These messages are handled by the broker and do not become application tasks.

A missed or late check, or a database connection error, fails the listening loop.
dispatcherd's existing producer recovery machinery then closes owned resources and
starts the listener again with fresh connections, a new conversation ID and the
same channel subscriptions. Cleanup attempts to retire the old registration; if
the database is unreachable, explicit cleanup may still be needed later.

Self-checks are reactive recovery for the local listener. They do not maintain a
shared worker-health record, renew a registration lease or remove other instances.
Set `max_connection_idle_seconds` to `None` in Python (`null` in YAML) to disable
periodic checks; the initial check still runs.

## Clean up after stopped processes

Normal shutdown removes a service's registration and closes its conversation.
Crashes can leave registrations and messages behind. Arrange a maintenance command
or scheduled job in your application to handle these cases; dispatcherd does not
install a periodic cleanup process.

### Service registrations: decide which instances to retire

`await broker.probe(timeout=2)` asks registered listeners to respond. Its result
contains `snapshot` (the registrations checked), `responded` and
`suspected_stale`. It does not remove anything. A busy, paused or disconnected
process can miss the deadline, so use application knowledge to decide whether an
instance should be removed. A response checks the listener's database communication,
not the health of its subprocess pool.

Pass the chosen conversation IDs to `broker.retire(selected_ids)` or
`await broker.aretire(selected_ids)`. Retirement closes their conversations and
discards pending notifications. Self-checks do not give registrations an age limit.
A crashed process's registration can remain until explicitly retired.

### Temporary replies: discard everything past its five-minute lifetime

Control calls and probes use short-lived reply conversations. These can be removed
unconditionally after expiry; there is no need to establish whether their callers
are still alive. Run `broker.cleanup_expired_replies()` or
`await broker.acleanup_expired_replies()` in your maintenance command. The result
lists the sending handles that were retired.

SQL Server exposes conversation endpoints in its
[`sys.conversation_endpoints` catalog](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-conversation-endpoints-transact-sql?view=sql-server-ver17).
The backend selects those using `DispatcherdReplyContract` whose `lifetime`
(expiration time) has passed, then closes both sides. This also finds a caller that
crashed partway through preparing its reply conversation. No reply registration
table or conversation-name prefix is needed to find these resources.

The five-minute lifetime stops further use of the reply address; the cleanup call
finishes releasing its resources. It excludes service conversations and unexpired
replies. SQL Server may retain records of closed endpoints for its own lifecycle,
but active endpoints and pending messages are released. If database connectivity
prevents shutdown cleanup, run the maintenance command after connectivity returns.

## Configuration reference

These options belong under `brokers.sqlserver` in the dispatcherd configuration:

| Option | Purpose and default |
| --- | --- |
| `config` | Connection options, including the required ODBC `connection_string`. Used for both synchronous and async database connections. |
| `channels` | Channels to listen on. Names are case-sensitive, cannot end with a space, and must occupy 1–256 UTF-16 code units (most common characters occupy one; some occupy two). The `sqlserver-reply:` prefix is reserved for return addresses. |
| `default_publish_channel` | Channel used when a publication omits one. With a single subscription, that channel is the default. With several, set this option or supply a channel when sending. |
| `node_id` | Optional label stored with the broker's registration. This is separate from the service `node_id` in Control replies and from the conversation ID used for targeted sends. |
| `wait_timeout_ms` | How long one database receive call waits for a message: 1–30000 milliseconds, default 1000. This is not the overall Control timeout. |
| `registration_timeout` | Deadline for receiving the initial self-check, or preparing a temporary reply conversation, in seconds; default 10. Individual connection and query timeouts also apply. |
| `max_connection_idle_seconds` | Idle interval before sending a self-check, in seconds; default 30. Set to `None` in Python or `null` in YAML to disable periodic checks. |
| `max_self_check_message_age_seconds` | Deadline for receiving a periodic self-check, in seconds; default 2. A missed or late check triggers listener recovery. |
| `max_message_bytes` | Size of dispatcherd message chunks, default 65536 bytes. Set to `None` in Python or `null` in YAML to disable chunking. |

### Async operation and shutdown

The service uses aioodbc with a dedicated receiving connection and a separate
connection for outgoing messages, including Control replies. Synchronous callers
use pyodbc. The two paths do not share connections.

ODBC itself is blocking: aioodbc runs its calls in threads. Connections created by
the async broker use a private two-thread executor so waiting for a message does
not occupy asyncio's shared default executor. Cancelling an async call cannot
instantly stop a database operation already running in a thread. The backend waits
for that operation to settle before closing or reusing its connection. Shutdown
can therefore take a receive interval or a database/network timeout. Receive waits
and SQL lock waits are bounded separately.

Use `close()` after synchronous operations and `await aclose()` after async ones.
If your code publishes through both families on one broker, close both. Only one
receive loop, either synchronous or async, can run on a broker at a time.

### Publishing through an application-owned connection

By default the broker creates its own connections. Listener connections and owned
async connections use autocommit, so each completed send becomes visible without
an application commit. Owned synchronous publishers also default to autocommit.

If you need a send to participate in an application transaction, supply a publishing
connection using one of these options:

- `sync_connection`: a pyodbc connection; alternatively, `sync_connection_factory`
  accepts a callable or dotted Python path that returns one.
- `async_connection`: an aioodbc connection; alternatively,
  `async_connection_factory` accepts an async callable or dotted Python path that
  returns one.

Factories receive the `config` dictionary as keyword arguments. Connections supplied
directly or through these factories remain application-owned: the broker does not
commit, roll back or close them. Your commit exposes the messages; rollback removes
them. Serialize any external use of the connection, configure its query timeout,
and handle rollback after errors. A sync connection is used only by sync methods;
an async connection is used only by async methods.

Use autocommit for connections supplying Control requests/replies or probes, where
the other process must see the message immediately. Listening and administrative
retirement use separate owned connections, so retiring a registration cannot commit
your publishing transaction.

## Publish from Django

The PostgreSQL broker's `connection_saver` is a demonstration of an application-owned
connection, not an adapter that imports Django. Both brokers accept a synchronous
connection factory. With SQL Server, the factory must return Django's underlying
**pyodbc connection**, rather than its database wrapper or cursor.

These examples target Microsoft's `mssql-django` backend with `ENGINE: "mssql"`
and its pyodbc driver. Newer versions also offer `mssql-python`; that driver is not
the connection type used by this broker. See Microsoft's
[driver selection documentation](https://learn.microsoft.com/en-us/sql/connect/python/mssql-django/select-database-driver?view=sql-server-ver17).
The [example helpers](../tools/sqlserver/django_publish.py) check the connection
type instead of assuming that every Django SQL Server backend uses pyodbc.

### Borrow the connection for a synchronous publication

Copy the example helpers into an application module, such as
`my_app/dispatcher_db.py`. Its `django_sqlserver_connection()` method opens Django's
connection if necessary and returns `connections[using].connection`. It leaves
credentials, autocommit and query timeouts under Django's control.

You can select it through dispatcherd configuration:

```python
CONFIG['brokers']['sqlserver']['sync_connection_factory'] = (
    'my_app.dispatcher_db.django_sqlserver_connection'
)
```

The helper defaults to Django's `default` database alias. To select another alias,
provide an application factory that calls it with `using='your_alias'`. Install
dispatcherd's SQL objects in that alias's database and grant the Django database
user the `dispatcherd_runtime` role. The service and publishers must all use this
same database.

Create and use a borrowed publisher within the same synchronous call and thread.
The broker caches its publishing connection for its own lifetime, while Django
can close or replace connections between requests. Do not keep a borrowed broker
in a global variable across requests. A new broker calling the factory gets the
current Django connection. Closing that broker does not close Django's connection.

### Commit application changes and notifications together

The example also provides `publish_in_transaction()`, which creates a short-lived
broker on Django's connection. It joins an existing transaction, or opens one if
there is none:

```python
import json
from django.db import transaction
from my_app.dispatcher_db import publish_in_transaction
from my_app.models import Job

with transaction.atomic(using='default'):
    job = Job.objects.create(status='pending')
    recipients = publish_in_transaction(
        'app_jobs',
        json.dumps({'task': 'my_app.tasks.process_job', 'args': [job.pk]}),
        using='default',
    )
```

SQL Server queues the notification as part of the same transaction as the ORM
write. The listener cannot receive it before commit, and rollback removes both.
This example broadcasts; `recipients` contains the conversation IDs sent to, and
can be empty if no service subscribes. Transactional publishing does not add a
job-delivery guarantee or change the channel's broadcast behavior.

There are two SQL Server details behind this helper:

- The broker's SQL uses ODBC `?` parameters, whereas Django's cursor wrapper expects
  Django-style parameters. Returning the raw pyodbc connection lets the broker use
  its existing SQL directly.
- Raw cursor calls bypass Django's error wrapper. The helper translates driver
  errors through that wrapper and uses `atomic(savepoint=False)` so a publication
  failure marks the surrounding transaction for rollback, even if the application
  catches the error. The send procedure uses `XACT_ABORT ON`; SQL errors can abort
  the enclosing transaction. Let that transaction roll back rather than attempting
  to continue using a nested savepoint after a database error.

When using the factory directly with `submit_task()`, let publishing exceptions
escape the outer `atomic()` block so Django rolls it back. The factory supplies a
connection; it does not wrap the subsequent publication in Django error handling.
The helper is the example for applications that need to catch publishing failures.

If publication uses a separate connection or database, use Django's
[`transaction.on_commit()`](https://docs.djangoproject.com/en/5.2/topics/db/transactions/#performing-actions-after-commit)
to defer it until the ORM write commits. That is a different guarantee: a later
publication failure cannot roll back the already committed application change.

### Async Django views and the dispatcherd service

Django's borrowed pyodbc connection belongs to its synchronous transaction and
thread. It cannot be passed as `async_connection`, and opening a separate aioodbc
connection does not join that transaction. From an async view, put the complete
ORM transaction and synchronous publication in one synchronous function, then call
it using `await sync_to_async(function, thread_sensitive=True)(...)`. Resolve the
Django connection inside that function. See Django's
[async transaction and thread guidance](https://docs.djangoproject.com/en/5.2/topics/async/).

The dispatcherd service continues to use its own aioodbc connections from the
broker's `config`. The Django factory applies only to synchronous publications;
it does not supply service connections. Use the ordinary independent broker
connections for Control requests and replies, which need immediate visibility.

## Database objects and implementation details

Setup uses fixed object names in the `dbo` schema. Use separate databases when you
need isolated sets of listeners and publishers.

| Objects | Use |
| --- | --- |
| `DispatcherdInbox`, `DispatcherdListener` | Shared incoming queue and its SQL service. Each dispatcherd listener receives only its own conversation's messages. |
| `DispatcherdOutbox`, `DispatcherdPublisher` | Queue and SQL service for the initiating side of conversations. |
| `DispatcherdPayload`, `DispatcherdContract` | Message type and contract for ordinary service notifications. |
| `DispatcherdListenerRegistry`, `DispatcherdSubscription` | Application metadata keyed by conversation ID, and channel-to-conversation membership. Endpoint handles are resolved from SQL Server's catalog. |
| `DispatcherdReplyListener`, `DispatcherdReplyContract` | SQL service and contract for temporary replies. The service shares the inbox queue. |
| `Dispatcherd*` stored procedures | Operations for registering, sending, receiving and cleaning up. Application users access these through the `dispatcherd_runtime` role. |

A listener's first message is its self-check. SQL Server creates the target
endpoint when that message arrives. Temporary reply conversations also exchange
an initial message before the return address is published. No SQL
queue, service or stored procedure is created per listener or Control call.
SQL Server's own conversation-end and error messages are handled by the backend;
they are never passed to dispatcherd as task payloads.

The reply procedures are `DispatcherdOpenReply`, `DispatcherdReplyReady`,
`DispatcherdSendReply`, `DispatcherdReceiveReply`, `DispatcherdCloseReply` and
`DispatcherdExpiredReplies`. Cleanup uses normal `END CONVERSATION` on both sides.
The [provisioning SQL](../dispatcherd/brokers/sqlserver_schema.sql) defines these
objects; the [Python implementation](../dispatcherd/brokers/sqlserver.py) calls them.

Control uses the optional broker methods `open_reply_endpoint(timeout)` and
`aopen_reply_endpoint(timeout)` to prepare the return address before sending.
Other dispatcherd brokers continue to use temporary reply channels when they do
not provide these methods.

### Rebuilding the runtime database

There is no schema migration or data-preservation path. Stop the applications,
create a fresh database (or remove and recreate all of this backend's SQL objects),
run `Broker.setup_database()` and restart services and publishers. Existing
registrations and queued messages are discarded. Reset the development database's
Docker volume when testing a changed schema.

## Run the integration tests

This checkout includes a database container and tests for the SQL Server backend.
Docker Compose uses the official SQL Server 2022 Linux image in Developer edition.
Use an x86-64 Docker host with at least 2 GB available for SQL Server itself.

Create local development credentials and export them for both Compose and pytest:

```sh
cp .env.sqlserver.example .env.sqlserver
# Edit .env.sqlserver and replace the password placeholder with a unique strong password.
set -a
. ./.env.sqlserver
set +a
```

The local credential file is Git-ignored. SQL Server requires a strong password
that meets its password complexity requirements. Set `MSSQL_PORT` to another
available port if localhost:14333 is already occupied. The port binds to localhost
only. Leave `MSSQL_HOST` unset to use localhost for host-side tests.

Start the database and wait for its SQL readiness check:

```sh
make sqlserver
# Equivalent:
# docker compose up -d --wait sqlserver
```

SQL Server is an opt-in Compose service. Existing PostgreSQL/demo commands do not
start it or require its password. Tests create/provision their `dispatcherd`
database and temporary test databases explicitly using the exported administrative
credentials. No application service or extra provisioning process needs to run.

With unixODBC and Microsoft ODBC Driver 18 installed on your host (as in application
setup step 1), install Python dependencies into the local virtual environment and
run the suite:

```sh
python3 -m venv .venv
.venv/bin/pip install -e '.[sqlserver]' pytest pytest-asyncio 'Django>=5.2,<5.3' 'mssql-django>=1.6,<3'
make test-sqlserver
# Equivalent pytest invocation:
# .venv/bin/pytest -q --confcutdir=tests/sqlserver tests/sqlserver
```

`make test-sqlserver` checks for credentials and the native driver before running,
and reports missing dependencies before pytest starts.
The `--confcutdir` option isolates this suite from PostgreSQL fixtures. Direct
pytest runs without credentials or the optional ODBC dependency may skip tests.

If you prefer not to install native ODBC packages on your host, this checkout also
provides a test-runner image with Microsoft ODBC Driver 18 and its own Python
virtual environment. It connects to the same Compose SQL Server service:

```sh
make test-sqlserver-docker
# Equivalent; starts SQL Server if necessary and waits for readiness:
# docker compose --profile sqlserver run --build --rm sqlserver-tests
```

The runner builds and installs this checkout's package, including its provisioning
SQL, and tests that installed package. It also installs Django and mssql-django
to test the application helpers against real ORM transactions. The runner uses port 1433 on the Compose
network regardless of the chosen host port.

Stop the database with `make stop-sqlserver` (or `docker compose stop sqlserver`).
Database files persist in the project's `sqlserver-data` volume across container
recreation. Changing the password environment variable does not reset an existing
volume's SA password; keep using the password with which that database was created.
To deliberately reset this test database, remove the SQL Server container and its
named volume, then start it again. Avoid `docker compose down` when you want to
leave this project's other services running.
