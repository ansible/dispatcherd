## Submitting tasks to run

The `dispatcherd.publish` module contains the relevant interface.
This describes the way to submit a task to be ran from code.

### Using Submit Task Method

The currently preferred way to submit a simple task to be ran
is documented in the [README.md](../README.md).

You may still want to decorate your methods, but you can avoid
attaching the old `delay` and `apply_async` methods using `decorate=False`.

```python
@task(decorate=False)
def test_method():
    return
```

Doing this _only_ adds the method to the dispatcherd internal registry.
You may want to do this to prevent submitting a task that is not registered by accident.

#### Additional Options

Some non-testing options are direct kwargs to `submit_task` which are:
 - queue: the queue to submit the task to
 - args: positional arguments to the task
 - kwargs: keyword arguments to the task
 - uuid: uuid4 string to identify the task
 - timeout: maximum time task is allowed to run
 - processor_options: options specific to a certain part of the dispatcherd code path

```python
from test_methods import print_hello

from dispatcherd.publish import submit_task
from dispatcherd.processors.delayer import Delayer

# After the setup() method has been called

submit_task(
    test_methods.print_hello,
    processor_options=[
        Delayer.Params(delay=3)
    ]
)
```

The `processor_options` can provide a list of objects which are created
by `.Params` from a given processor class.
The processor classes are also used by the background dispatcherd service
when dispatching the task.
These are parameters passed to that specific class,
for the processing of that particular task.
The `Delayer`, for example, may delay starting task for the specified
number of seconds.

### Getting Confirmation

The `submit_task` is fire-and-forget. This is particularly so for pg_notify.
Because as long as postgres is communicating, it sends the message to the channel
and that is treated as sucessfully submitting a task.
This is true even if there are no services listening to the channel,
in which case nothing will happen.

If you want confirmation of submission by a service (could be more than 1 service),
then control tasks offers an option.

### Old Celery Way

This mimics the way that the Celery library works.
For now, this is still in dispatcherd but may be removed in the future.
This requires that the method is decorated with `@task()` or
`@task(decorate=True)`, as `decorate=False` prevents these methods
from being attached.

```python
from test_methods import print_hello

# After the setup() method has been called

print_hello.delay()
```

This method does not take any args or kwargs, but if it did, you would
pass those directly as in, `.delay(*args, **kwargs)`.

Also valid:

```python
from test_methods import print_hello

# After the setup() method has been called

print_hello.apply_async(args=[], kwargs={})
```

The difference is that `apply_async` takes both args and kwargs as kwargs themselves,
and allows for additional configuration parameters to come after those.
