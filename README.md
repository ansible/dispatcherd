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

Start the dispatcher.

```
pip install prometheus_client

$ curl http://localhost:8070

curl http://localhost:8070
# HELP dispatcher_messages_received_total Number of messages received by dispatchermain
# TYPE dispatcher_messages_received_total counter
dispatcher_messages_received_total 88.0
# HELP dispatcher_control_messages_count_total Number of control messages received.
# TYPE dispatcher_control_messages_count_total counter
dispatcher_control_messages_count_total 10.0
# HELP dispatcher_worker_created_at Creation time of worker
# TYPE dispatcher_worker_created_at gauge
dispatcher_worker_created_at{worker_index="0"} 286576.365272104
dispatcher_worker_created_at{worker_index="1"} 286576.365368035
dispatcher_worker_created_at{worker_index="2"} 286578.814706941
dispatcher_worker_created_at{worker_index="3"} 286578.817639637
dispatcher_worker_created_at{worker_index="4"} 286578.820265243
dispatcher_worker_created_at{worker_index="5"} 286578.822075874
dispatcher_worker_created_at{worker_index="6"} 286578.824725725
dispatcher_worker_created_at{worker_index="7"} 286578.837810563
dispatcher_worker_created_at{worker_index="8"} 286578.844329095
dispatcher_worker_created_at{worker_index="9"} 286578.863277972
dispatcher_worker_created_at{worker_index="10"} 286578.878555905
dispatcher_worker_created_at{worker_index="11"} 286579.36656921
# HELP dispatcher_worker_finished_count Finished count of tasks by the worker
# TYPE dispatcher_worker_finished_count gauge
dispatcher_worker_finished_count{worker_index="0"} 2.0
dispatcher_worker_finished_count{worker_index="1"} 1.0
dispatcher_worker_finished_count{worker_index="2"} 1.0
dispatcher_worker_finished_count{worker_index="3"} 7.0
dispatcher_worker_finished_count{worker_index="4"} 8.0
dispatcher_worker_finished_count{worker_index="5"} 1.0
dispatcher_worker_finished_count{worker_index="6"} 0.0
dispatcher_worker_finished_count{worker_index="7"} 0.0
dispatcher_worker_finished_count{worker_index="8"} 0.0
dispatcher_worker_finished_count{worker_index="9"} 2.0
dispatcher_worker_finished_count{worker_index="10"} 1.0
dispatcher_worker_finished_count{worker_index="11"} 1.0
# HELP dispatcher_worker_status Status of worker.
# TYPE dispatcher_worker_status gauge
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="0"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="0"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="1"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="1"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="2"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="2"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="3"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="3"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="4"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="4"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="5"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="5"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="6"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="6"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="7"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="7"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="8"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="8"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="9"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="9"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="10"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="10"} 0.0
dispatcher_worker_status{dispatcher_worker_status="error",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="exited",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="initialized",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="ready",worker_index="11"} 1.0
dispatcher_worker_status{dispatcher_worker_status="retired",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="spawned",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="starting",worker_index="11"} 0.0
dispatcher_worker_status{dispatcher_worker_status="stopping",worker_index="11"} 0.0
```
