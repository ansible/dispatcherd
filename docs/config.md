## dispatcherd Configuration

Why is configuration needed? Consider doing this, which uses the demo content:

```
PYTHONPATH=$PYTHONPATH:tools/ python -c "from tools.test_methods import sleep_function; sleep_function.delay()"
```

This will result in an error:

> Dispatcherd not configured, set DISPATCHERD_CONFIG_FILE or call dispatcherd.config.setup

This errors because dispatcherd does not know how to connect to a message broker.
For the PostgreSQL broker, that is the postgres host, user, password, and notification channel.
For the SQL Server broker, it is the server, database, credentials, and subscribed channels.

### Ways to configure

#### From file

The demo runs using the `dispatcher.yml` config file at the top-level of this repo.

You can do the same thing in python by:

```python
from dispatcherd.config import setup

setup(file_path='dispatcher.yml')
```

This is used by the demo's test script at `tools/write_messages.py`,
which acts as a "publisher", meaning that it submits tasks to the message
broker to be ran.
Using `setup` ensures that both the service (`dispatcherd`) and the publisher
are using the same configuration.

##### With Environment variable

Alternatively, the `DISPATCHERD_CONFIG_FILE` environment variable can point to a file. Example:

```
DISPATCHERD_CONFIG_FILE=dispatcher.yml dispatcherd
```

#### From a dictionary

Calling `setup(config)`, where `config` is a python dictionary is
equivelent to dumping the `config` to a yaml file and using that as
the file-based config.

### Configuration Contents

At the top-level, config is broken down by either the process that uses that section,
or brokers, which is a shared resources used by multiple processes.

At the level below that, the config gives instructions for creating python objects.
The module `dispatcherd.factories` has the task of creating those objects from settings.
The design goal is to have a little possible divergence from the settings structure
and the class structure in the code.

The general structure is:

```yaml
---
version: # number
service:
  pool_kwargs:
    # options
brokers:
  pg_notify:
    # options
  sql_server:
    # options
producers:
  ProducerClass:
    # options
publish:
  # options
worker:
  # options
```

When providing `pool_kwargs`, those are the kwargs passed to `WorkerPool`, for example.

#### Version

The version field is mandatory and must match the current config in the library.
This is validated against current code and saved in the [schema.json](../schema.json) file.

The version will be bumped when any breaking change happens.

You can re-generate the schema after making changes by running:

```
python tools/gen_schema.py > schema.json
```

#### Brokers

Brokers relay messages which give instructions about code to run.
Dispatcherd includes PostgreSQL `pg_notify` and SQL Server table brokers.
Both are selected by their key under `brokers`.

The sub-options become python `kwargs` passed to the broker class `Broker`.
The PostgreSQL options are documented in
[dispatcherd.brokers.pg_notify](dispatcherd/brokers/pg_notify.py).
The SQL Server options are documented in
[dispatcherd.brokers.sql_server](dispatcherd/brokers/sql_server.py).

The broker classes have methods that allow for submitting messages
and reading messages.

##### SQL Server table broker

Install the Python extra and Microsoft ODBC Driver 18 for SQL Server:

```bash
pip install 'dispatcherd[sql_server]'
```

Create the `dispatcherd` database and message table with
[`tools/sql_server/setup.sql`](../tools/sql_server/setup.sql), then configure the
broker. Supply credentials through your deployment's secret management system.

```yaml
brokers:
  sql_server:
    config:
      server: sqlserver.example.com
      port: 1433
      database: dispatcherd
      user: dispatcherd_app
      password: <secret>
      driver: ODBC Driver 18 for SQL Server
      trust_server_certificate: "no"
      connect_timeout_seconds: 15
      query_timeout_seconds: 30
    channels:
      - metrics
      - dashboard
      - maintenance
    poll_interval_seconds: 0.25
```

Publishers and dispatcherd services must use the same database, broker key,
and channel names. The broker stores the unchanged JSON payload in
`dbo.dispatcherd_messages` and atomically removes rows when a consumer claims
them. Messages are delivered at most once after a claim; applications that
need recovery should keep task state in their own database and reconcile
pending work. Connection attempts default to a 15 second timeout, and SQL
statements default to a 30 second timeout; both values must be positive and can
be tuned in the broker `config`.

#### Service

This configures the background task service.

The `pool_kwargs` options will correspond to the `WorkerPool` class
[dispatcherd.pool](dispatcherd/pool.py).
Process management options will be added to this section later.

These options are mainly concerned with worker
management. For instance, auto-scaling options will be here,
like worker count, etc.
You can also tune `results_read_timeout`, which configures how long the pool waits
for items on the finished queue before re-checking shutdown conditions. The default
is to wait indefinitely (set the option to `null`), but a float value can be provided
to limit how long the queue read blocks before re-evaluating shutdown criteria.

`worker_max_lifetime_seconds` controls how long a worker process may live before
it will be retired. The default is 4 hours. Setting it to `null` disables
automatic retirement, which is not recommended for long-running services.

#### Producers

These are "producers of tasks" in the dispatcherd service.

For every listed broker, a `BrokeredProducer` is automatically
created. That means that tasks may be produced from the messaging
system that the dispatcherd service is listening to.

The others are:

- `ScheduledProducer` - submits tasks every certain number of seconds
- `OnStartProducer` - runs tasks once after starting

#### Publish

Additional options for publishers (task submitters).

#### Worker

This allows you to configure a `worker_cls`, pointing to
an importable location of your own class.
Syntax:

```yaml
worker:
  worker_cls: my_app.AppWorker
  worker_kwargs:
    idle_timeout: 120
```

Your class should subclass from `dispatcherd.worker.task.TaskWorker`.
This allows you to define callbacks on your class.
You can get a sense of what callbacks are possible
by looking at `TaskWorker` in the [protocol module](../dispatcherd/protocols.py).

See the [example class with callbacks](../tests/data/callbacks.py) that is
used for testing this feature.

The `idle_timeout` is a number of seconds which determines how long
after no new tasks are received that the `on_idle` callback will be called.
That can be useful for connection keep-alive work and such.
