<!-- License Badge -->
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/ansible/dispatcher/blob/main/LICENSE)

## Dispatcher

The dispatcher is a service to run python tasks in subprocesses.
Its philosophy is to have a limited scope
as a "local" runner of background tasks, but to be composable
so that it can be "wrapped" easily to enable clustering and
distributed task management by apps using it.

Licensed under [Apache Software License 2.0](LICENSE)

### Usage

You have a postgres server configured and a python project.
You will use dispatcher to trigger a background task over pg_notify.

Both your *background dispatcher service* and your *task publisher* process must have
python configured so that your task is importable.

For more options, see [docs/usage](docs/usage.md).

#### Library

The dispatcher `@task()` decorator is used to register tasks.

The [tests/data/methods.py](tests/data/methods.py) module defines some
dispatcher tasks and the pg_notify channels they will be sent over.

```python
from dispatcher.publish import task

@task(queue='test_channel')
def print_hello():
    print('hello world!!')
```

Additionally, you need to configure dispatcher somewhere in your import path.
This tells dispatcher how to submit tasks to be ran.

```python
from dispatcher.config import setup

config = {
    "producers": {
        "brokers": {
            "pg_notify": {"conninfo": "dbname=postgres user=postgres"},
            "channels": [
                "test_channel",
            ],
        },
    },
    "pool": {"max_workers": 4},
}
setup(config)
```

For more on how to set up and the allowed options in the config,
see the section [config](docs/config.md) docs.

#### Dispatcher service

The dispatcher service needs to be running before you submit tasks.
This does not make any attempts at message durability or confirmation.
If you submit a task in an outage of the service, it will be dropped.

There are 2 ways to run the dispatcher service:

- Importing and running (code snippet below)
- A CLI entrypoint `dispatcher-standalone` for demo purposes

```python
from dispatcher import run_service

# After the setup() method has been called

run_service()
```

Configuration tells how to connect to postgres, and what channel(s) to listen to.
The demo has this in `dispatcher.yml`, which includes listening to `test_channel`.
That matches the `@task` in the library.

#### Publisher

This assumes you configured python so that `print_hello` is importable
from the `test_methods` python module.
This method does not take any args or kwargs, but if it did, you would
pass those directly as in, `.delay(*args, **kwargs)`.

The following code will submit `print_hello` to run in the background dispatcher service.

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

### Manual Demo

General setup:

```
pip install -e .[pg_notify]
make postgres
```

You need to have 2 terminal tabs open to run this.

```
# tab 1
PYTHONPATH=$PYTHONPATH:. dispatcher-standalone
# tab 2
./run_demo.py
```

This will run the dispatcher with schedules, and process a burst of messages
that give instructions to run tasks.

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

There is documentation of the message formats used by the dispatcher
in [docs/message_formats.md](docs/message_formats.md). Some of these are internal,
but some messages are what goes over the user-defined brokers (pg_notify).

## Contributing

We ask all of our community members and contributors to adhere to the [Ansible code of conduct](https://docs.ansible.com/ansible/latest/community/code_of_conduct.html).
If you have questions or need assistance, please reach out to our community team at <codeofconduct@ansible.com>

Refer to the [Contributing guide](docs/contributing.md) for further information.

## Communication

See the [Communication](https://github.com/ansible/dispatcher/blob/main/docs/contributing.md#communication) section of the
Contributing guide to find out how to get help and contact us.

For more information about getting in touch, see the
[Ansible communication guide](https://docs.ansible.com/ansible/devel/community/communication.html).

## Credits

Dispatcher is sponsored by [Red Hat, Inc](https://www.redhat.com).
