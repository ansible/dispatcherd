## Task Options

You can specify additional details about the behavior of a task
by applying additional options.

There are normally 2 days to do this

- pass to the `@task` decorator
- pass to the `.apply_async` method for a one-off use

The structure of use of those options is illustrated below,
with `options` being keyword-based additional options.

```python
from dispatcherd.publish import task

@task(queue='test_channel', **options)
def print_hello():
    print('hello world!!')
```

For example, to set a timeout of 1 second on the task:

```python
from dispatcherd.publish import task

@task(queue='test_channel', **options)
def print_hello():
    print('hello world!!')
```

For the one-off use, `.apply_async` can take options,
but `.delay` cannot, because of the argument structure.
Using `.delay` inherently runs the task with the task default options.

```python
from test_methods import print_hello

print_hello.apply_async(args=[], kwargs={}, **options)
```

For the timeout seconds example:

```python
from test_methods import print_hello

print_hello.apply_async(args=[], kwargs={}, timeout=2)
```

The `apply_async` options will take precedence over the
task default options (those passed into the decorator).

### Task Options Manifest

This section documents specific options.
These follow a "standard" pattern, meaning that they
can be used in both of the ways described above.

#### Bind

If `bind=True` is passed (default is `False`), then
additional argument is inserted at the start of the
argument list to the method. Like:

```python
@task(bind=True)
def hello_world(dispatcher, *args, **kwargs):
    print(f'I see the dispatcher object {dispatcher}')
```

The `dispatcher` object contains public methods
which allow interaction with the parent process.
Available methods will expand in the future,
right now it offers:

- `uuid` - the internal id of this task call in dispatcher
- `worker_id` - the id of the worker running this task
- `control` - runs a control-and-reply command against its own parent process

Using the `dispatcher.control` interface on the bound object is
an more efficient alternative to communication over the broker.
It also allows tasks to dispatch follow-up tasks in the local service.

More complex examples can be found in `tests.data.methods`.
The `schedules_another_task` example shows how this can be used
to have a task start another task.

#### Queue

The queue or channel this task is submitted to.
For instance, the pg_notify channel.
This can be a callable to get this dynamically.

#### on_duplicate

This option helps to manage capacity, controlling

- task "shedding"
- task queuing

Depending on the value, a task submission will be ignored
if certain conditions are met, "shedding", or queued if all
workers are busy.

- parallel - multiple tasks (running the given `@task` method) are allowed at the same time. Tasks queue if no free workers are available.
- discard - if a task is already being ran or is queued, any new submissions of this task are ignored.
- serial - only 1 task (running the given method) will be ran at a single time in the local dispatcher service. Additional submissions are queued, so all submissions will be ran eventually.
- queue_one - for idempotent tasks, only 1 task (running the given method) will be ran at a single time, and an additional submission is queued. However, only 1 task will be held in the queue, and additional submissions are discarded. This assures _timely_ running of an idempotent task.

### Unusual Options

These do not follow the standard pattern for some reason.
Usually for testing.

#### registry

The dispatcher uses a global task registry.
To enable isolated testing `@task` can take a custom
(meaning non-global) registry.

There is no real multi-registry feature,
and additional custom code hooks would be needed to make this work.
