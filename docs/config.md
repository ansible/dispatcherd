## dispatcherd Configuration

Why is configuration needed? Consider doing this, which uses the demo content:

```
PYTHONPATH=$PYTHONPATH:tools/ python -c "from tools.test_methods import sleep_function; sleep_function.delay()"
```

This will result in an error:

> Dispatcherd not configured, set DISPATCHERD_CONFIG_FILE or call dispatcherd.config.setup

This errors because dispatcherd does not know how to connect to a message broker.
For the demo, that would be the postgres host, user, password, etc.,
as well as the pg_notify channel to send the message to.

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
Right now the only broker available is pg_notify.

The sub-options become python `kwargs` passed to the broker class `Broker`.
For now, you will just have to read the code to see what those options are
at [dispatcherd.brokers.pg_notify](dispatcherd/brokers/pg_notify.py).

The broker classes have methods that allow for submitting messages
and reading messages.

#### Service

This configures the background task service.

The `pool_kwargs` options will correspond to the `WorkerPool` class
[dispatcherd.pool](dispatcherd/pool.py).
Process management options will be added to this section later.

These options are mainly concerned with worker
management. For instance, auto-scaling options will be here,
like worker count, etc.

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
