<!-- License Badge -->
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/ansible/dispatcherd/blob/main/LICENSE)

## Dispatcherd

The dispatcherd is a service to run python tasks in subprocesses,
designed specifically to work well with pg_notify,
but intended to be extensible to other message delivery means.
Its philosophy is to have a limited scope
as a "local" runner of background tasks, but to be composable
so that it can be "wrapped" easily to enable clustering and
distributed task management by apps using it.

> [!WARNING]
> This project is in initial development. Expect many changes, including name, paths, and CLIs.

Licensed under [Apache Software License 2.0](LICENSE)

### Usage

You have a postgres server configured and a python project.
You will use dispatcherd to trigger a background task over pg_notify.
Both your *background dispatcherd service* and your *task publisher* process must have
python configured so that your task is importable. Instructions are broken into 3 steps:

1. **Library** - Configure dispatcherd, mark the python methods you will run with it
2. **dispatcherd service** - Start your background task service, it will start listening
3. **Publisher** - From some other script, submit tasks to be ran

In the "Manual Demo" section, an runnable example of this is given.

#### Library

The dispatcherd `@task()` decorator is used to register tasks.
The [tests/data/methods.py](tests/data/methods.py) module defines some
dispatcherd tasks.

The decorator accepts some kwargs (like `queue` below) that will affect task behavior,
see [docs/task_options.md](docs/task_options.md).

```python
from dispatcherd.publish import task

@task(queue='test_channel')
def print_hello():
    print('hello world!!')
```

Configure dispatcherd somewhere in your import path or before running the service.
This tells dispatcherd how to submit tasks to be ran.

```python
from dispatcherd.config import setup

config = {
    "producers": {
        "brokers": {
            "pg_notify": {
                "conninfo": "dbname=postgres user=postgres"
                "channels": [
                    "test_channel",
                ],
            },
        },
    },
    "pool": {"max_workers": 4},
}
setup(config)
```

For more on how to set up and the allowed options in the config,
see the section [config](docs/config.md) docs.
The `queue` passed to `@task` needs to match a pg_notify channel in the `config`.
It is often useful to have different workers listen to different sets of channels.

#### dispatcherd service

The dispatcherd service needs to be running before you submit tasks.
This does not make any attempts at message durability or confirmation.
If you submit a task in an outage of the service, it will be dropped.

There are 2 ways to run the dispatcherd service:

- Importing and running (code snippet below)
- A CLI entrypoint `dispatcherd` for demo purposes

```python
from dispatcherd import run_service

# After the setup() method has been called

run_service()
```

Configuration tells how to connect to postgres, and what channel(s) to listen to.

#### Publisher

This assumes you configured python so that `print_hello` is importable
from the `test_methods` python module.
This method does not take any args or kwargs, but if it did, you would
pass those directly as in, `.delay(*args, **kwargs)`.

The following code will submit `print_hello` to run in the background dispatcherd service.

```python
from test_methods import print_hello

# After the setup() method has been called

print_hello.delay()
```

Also valid:

```python
from test_methods import print_hello

# After the setup() method has been called

print_hello.apply_async(args=[], kwargs={})
```

The difference is that `apply_async` takes both args and kwargs as kwargs themselves,
and allows for additional configuration parameters to come after those.

### Manual Demos

#### Service in foreground

Initial setup:

```
pip install -e .[pg_notify]
```

To experience running a `dispatcherd` service, you can try this:

```
make postgres
dispatcherd
```

The `dispatcherd` entrypoint will look for a config file in the current
directory if not otherwise specified, which is [dispatcher.yml](dispatcher.yml)
in this case. You can see it running some schedules and listening.

Ctl+c to stop that server.

#### Two nodes in background

The following will start up postgres, then start up 2 dispatcherd services.
It should take a few seconds, mainly waiting for postgres.

```
make demo
```

After it completes `docker ps -a` should show `dispatcherd1` and `dispatcherd2`
containers as well as postgres. You can see logs via `docker logs dispatcherd1`.
These will accept task submissions. Submit a lot of tasks as a python
task publisher with the `run_demo.py` script. To get accurate replies,
we need to specify that `2` replies are expected because we are
communicating with 2 background task services.

```
./run_demo.py 2
```

You can talk to these services over postgres with `dispatcherctl`,
using the same local `dispatcher.yml` config.

```
dispatcherctl running
dispatcherctl workers
```

The "running" command will likely show scheduled tasks and leftover tasks from the demo.
For demo, the `uuid` and `task` options allow doing filtering.

```
dispatcherctl running --task=tests.data.methods.sleep_function
```

This would show any specific instance of `tests.data.methods.sleep_function` currently running.

### Running Tests

Most tests (except for tests/unit/) require postgres to be running.

```
pip install -r requirements_dev.txt
make postgres
pytest tests/
```

### Background

This is intended to be a working space for prototyping a code split of:

<https://github.com/ansible/awx/tree/devel/awx/main/dispatch>

As a part of doing the split, we also want to resolve a number of
long-standing design and sustainability issues, thus, asyncio.
For a little more background see [docs/design_notes.md](docs/design_notes.md).

There is documentation of the message formats used by the dispatcherd
in [docs/message_formats.md](docs/message_formats.md). Some of these are internal,
but some messages are what goes over the user-defined brokers (pg_notify).
You can trigger tasks using your own "publisher" code as an alternative
to attached methods like `.apply_async`. Doing this requires connecting
to postges and submitting a pg_notify message with JSON data
that conforms to the expected format.
The `./run_demo.py` script shows examples of this, but borrows some
connection and config utilities to help.

## Contributing

We ask all of our community members and contributors to adhere to the [Ansible code of conduct](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html).
If you have questions or need assistance, please reach out to our community team at <codeofconduct@ansible.com>

Refer to the [Contributing guide](docs/contributing.md) for further information.

## Communication

See the [Communication](https://github.com/ansible/dispatcherd/blob/main/docs/contributing.md#communication) section of the
Contributing guide to find out how to get help and contact us.

For more information about getting in touch, see the
[Ansible communication guide](https://docs.ansible.com/ansible/devel/community/communication.html).

## Credits

Dispatcherd is sponsored by [Red Hat, Inc](https://www.redhat.com).

## Metrics

You can run a demo of the metrics server. In your first terminal tab, run:

```
pip install .[pg_notify,metrics]
dispatcherd
```

In another tab run:

```
curl http://localhost:8070
```

This should report metrics in the following general format:

```
$ curl http://localhost:8070
# HELP dispatcher_messages_received_total Number of messages received by dispatchermain
# TYPE dispatcher_messages_received_total counter
dispatcher_messages_received_total 263.0
# HELP dispatcher_control_messages_count_total Number of control messages received.
# TYPE dispatcher_control_messages_count_total counter
dispatcher_control_messages_count_total 0.0
# HELP dispatcher_worker_count_total Number of workers running.
# TYPE dispatcher_worker_count_total counter
dispatcher_worker_count_total 3.0
```

We expect to add more metrics in the future.
