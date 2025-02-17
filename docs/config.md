## Dispatcher Configuration

Why is configuration needed? Consider doing this, which uses the demo content:

```
PYTHONPATH=$PYTHONPATH:tools/ python -c "from tools.test_methods import sleep_function; sleep_function.delay()"
```

This will result in an error:

> Dispatcher not configured, set DISPATCHER_CONFIG_FILE or call dispatcher.config.setup

This is an error because dispatcher does not have information to connect to a message broker.
In the case of postgres, that information is the connection information (host, user, password, etc)
as well as the pg_notify channel to send the message to.

### Ways to configure

#### From file

The provided entrypoint `dispatcher-standalone` can only use a file, which is how the demo works.
The demo runs using the `dispatcher.yml` config file at the top-level of this repo.

You can do the same thing in python by:

```python
from dispatcher.config import setup

setup(file_path='dispatcher.yml')
```

This approach is used by the demo's test script at `tools/write_messages.py`,
which acts as a "publisher", meaning that it submits tasks over the message
broker to be ran.
This setup ensures that both the service (`dispatcher-standalone`) and the publisher
are using the same configuration.

#### From a dictionary

Calling `setup(config)`, where `config` is a python dictionary is
equivelent to dumping the `config` to a yaml file and using that as
the file-based config.

### Configuration Contents

At the top-level, config is broken down by either the process that uses that section,
or brokers, which is a shared resources used by multiple processes.

At the level below that, the config gives instructions for creating python objects.
The module `dispatcher.factories` has the task of creating those objects from settings.
The design goal is to have a little possible divergence from the settings structure
and the class structure in the code.

The general structure is:

```yaml
---
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
```

When providing `pool_kwargs`, those are the kwargs passed to `WorkerPool`, for example.

#### Brokers

Brokers relay messages which give instructions about code to run.
Right now the only broker available is pg_notify.

The sub-options become python `kwargs` passed to the broker classes
`AsyncBroker` and `SyncBroker`, for the sychronous and asyncio versions
of the broker.
For now, you will just have to read the code to see what those options are
at [dispatcher.brokers.pg_notify](dispatcher/brokers/pg_notify.py).

The broker classes have methods that allow for submitting messages
and reading messages.

#### Service

This configures the background task service.

The `pool_kwargs` options will correspond to the `WorkerPool` class
[dispatcher.pool](dispatcher/pool.py).
Process management options will be added to this section later.

These options are mainly concerned with worker
management. For instance, auto-scaling options will be here,
like worker count, etc.

#### Producers

These are "producers of tasks" in the dispatcher service.

For every listed broker, a `BrokeredProducer` is automatically
created. That means that tasks may be produced from the messaging
system that the dispatcher service is listening to.

The other current use case is `ScheduledProducer`,
which submits tasks every certain number of seconds.

#### Publish

Additional options for publishers (task submitters).
