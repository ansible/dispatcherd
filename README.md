# dispatcher
Working space for dispatcher prototyping

This is firstly intended to be a code split of:

https://github.com/ansible/awx/tree/devel/awx/main/dispatch

As a part of doing the split, we also want to resolve a number of
long-standing design and sustainability issues, thus, asyncio.

### Usage

You have a postgres server configured and a python project.
You will use dispatcher to trigger a background task over pg_notify.

Both your *background dispatcher service* and your *task publisher* process must have
python configured so that your task is importable.

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

There are 2 ways to run the dispatcher:
 - Importing and running
 - A CLI entrypoint `dispatcher-standalone` for demo purposes

The dispatcher ultimately requires code hooks anyway,
so any convienience the CLI offers is not very relevant
for running things in production.

Configuration must be provided so it knows how to connect to postgres,
and what channel to listen to. The demo has this in `dispatcher.yml`.
When importing and running, you just pass your configuration directly.

The demo config contains a list of channels to listen to which includes `test_channel`.

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
