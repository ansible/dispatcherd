<!-- License Badge -->
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/ansible/dispatcher/blob/main/LICENSE)

Working space for dispatcher prototyping

This is firstly intended to be a code split of:

<https://github.com/ansible/awx/tree/devel/awx/main/dispatch>

As a part of doing the split, we also want to resolve a number of
long-standing design and sustainability issues, thus, asyncio.

Licensed under [Apache Software License 2.0](LICENSE)

### Usage

You have a postgres server configured and a python project.
You will use dispatcher to trigger a background task over pg_notify.

Both your *background dispatcher service* and your *task publisher* process must have
python configured so that your task is importable.

For more options, see `docs/usage.md`.

#### Library

The dispatcher `@task()` decorator is used to register tasks.

See the `tools/test_methods.py` module.
This defines a dispatcher task and the pg_notify channel it will be sent over.

```python
from dispatcher.publish import task

@task(queue='test_channel')
def print_hello():
    print('hello world!!')
```

#### Dispatcher service

The dispatcher service needs to be running before you submit tasks.
This does not make any attempts at message durability or confirmation.
If you submit a task in an outage of the service, it will be dropped.

There are 2 ways to run the dispatcher service:

- Importing and running (code snippet below)
- A CLI entrypoint `dispatcher-standalone` for demo purposes

```python
from dispatcher.config import setup
from dispatcher import run_service

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

print_hello.delay()
```

Also valid:

```python
from test_methods import print_hello

print_hello.apply_async(args=[], kwargs={})
```

The difference is that `apply_async` takes both args and kwargs as kwargs themselves,
and allows for additional configuration parameters to come after those.

As of writing, this only works if you have a Django connection configured.
You can manually pass configuration info (as in the demo) for non-Django use.

### Manual Demo

You need to have 2 terminal tabs open to run this.

```
# tab 1
make postgres
PYTHONPATH=$PYTHONPATH:tools/ dispatcher-standalone
# tab 2
python tools/write_messages.py
```

This will run the dispatcher with schedules, and process a burst of messages
that give instructions to run tasks.

### Running Tests

A structure has been set up for integration tests.
The word "integration" only means that postgres must be running.

```
pip install -r requirements_dev.txt
make postgres
py.test tests/
```

This accomplishes the most basic of starting and shutting down.
With no tasks submitted, it should record running 0 tasks,
and with a task submitted, it records running 1 task.

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
