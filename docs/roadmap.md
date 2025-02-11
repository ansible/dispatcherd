
## Roadmap, planning

Here, we will maintain a list of features.
These may be converted into issues later.
The main goal is to not forget about them.

I will break this down into 2 categories.
The first category is pre-alpha.
I will assume that all commits will be squashed and this will be
moved to a new repo. At that point it will become public.
Everything before it becomes public are pre-alpha things to do.

### Pre-alpha

#### Track more data with new types

The AWX dispatcher had a model where a task could have certain parameters.
Like, imagine that we throw a generalized task timeout in.

The design of this was a little choppy, because the `@task` decorator
would declare these paramters.

Yet these parameters would be send in the pg_notify JSON data.

This doesn't really fit the model that we want.
We want it to be _impossible_ to run a task with parameters
other than what are declared on `@task`

Think of it this way, there are

- runtime arguments (or parameters)
- configuration parameters

Something like a task timeout is a configuration parameter.
This implies that when we register a method with `@task`
we have to **save it in a registry**.

When the dispatcher gets a message saying to run a task,
the most correct thing to do is look that task up in the registry.
This reduces the JSON data passed, and makes a more consistent
source-of-truth.

But this means that we need a registry, and way before that,
we need to introduce a type for methods that will be called.

Additionally, the next big objective is that we want
detailed tracking timing for every time a task is called.
This goes with the call, not the task.

So the new types we probably want are:

- `Task`
- `Call`

This is in addition to the `PoolWorker` which is the worker that
runs the task.

The `Call` will track the lifecycle of the call.
The `PoolWorker` will reference the `Call` it is running.
The `Call` will reference the `Task` it is a call of.

We don't need/want to pass any of this through the IPC queue
to the worker, we only want it for the main dispatcher.
This will mainly be useful as we ask the dispatcher to respond
with stats about what it has been running.

Also... I see this as a mechanism to write integration tests.
We can submit tasks, wait for a signal they finished,
and then get the work history from the dispatcher as it ran those.

We should look at the `Call` class as corresponding to a log record.
This should have the call details, identifiers, and mostly be a
record of the call lifecycle. This is mostly log-like, and should have
mostly scalar type data of floats, strings, ids, etc.

#### Finish integrating publisher logic

The content existing in `ansible_dispatcher.publish` is mostly not connected.

What's interesting here is that `ansible_dispatcher.publish` should import
from the broker module.
That gets hard to manage with multiple connections (ala Django).
But some version of it we should do...

#### Finish integrating the worker loop

Overlapping with the publisher stuff, the `ansible_dispatcher.publish` should
get the method name, and the args, import the method, and run it.

This requires moving more code in from AWX

<https://github.com/ansible/awx/blob/devel/awx/main/dispatch/worker/task.py>

That has

- importing logic
- calling logic
- supporting stuff to include timings
- signal handling
- exception handling

### Post-alpha

#### Conditional skipping logic on publishing

AWX uses sqlite3 for unit tests, which would error on async tasks.
Because of this, it did not publish a message if `is_testing` was True.
It's not reasonable for us to implement that same thing here, and
we will likely need some callback approach.

So the ask here is that we have some app-wide configuration,
which can inspect a message _before publishing_ and take some action,
or possibly cancel the NOTIFY.

Probably not good coding practice generally, but probably useful.

#### Feature branch to integrate with AWX

Make AWX run using this library, this should be an early goal in this stage.

#### Worker and Broker Self-Checks

A moderate version of this was proposed in:

<https://github.com/ansible/awx/pull/14749>

In grand conclusion, there is no way to assure that the LISTSEN connection
is not dropped.
Worse, when it is dropped, we may get no notification.
Astonishingly, there appears to be no way around this.

Because of this, the ultimate option of last-resource must be taken.
That means that we can only assure health of a connection of a worker
by experiential means.

To know if a connection works, you must publish a control message and receive it.
To know if a worker is alive, you must send a message and receive a reply.

Because of this knowledge, the new ansible_dispatcher library must just straight to this eventuality.
Implement checks for brokers and workers based on send-and-receive.
This can be done fully with asynio patterns.

For the issues related to AWX 14749, we also need means to recycle connections
in cases where we fail to receive check messages.

#### Worker Allocation Cookbook

Several very practical problems are not intended to ever be solved by the dispatcher.
However, for someone using postgres or any other modern database,
combined with the dispatcher, they have the ability to solve these problems.

<https://github.com/ansible/awx/issues/11997>

Breakdown of those problems:

1. Have a node in the cluster, any node, process a task
2. Have a periodic task run, anywhere in the cluster, at a certain frequency

The solution for (1) is to add an entry to a table when submitting the task.
Then depending on the use case, there are 2 decent options:

- broadcast a task asking any willing node to run the task, get lock, if lock is taken, bail
- run a periodic task that will use `select_for_update` to get entries and mark as received

The solution for (2) in AWX uses the Solo model to track a `datetime`.
This is self-obviously needed for the feature of _user_ schedules.

#### Task Timeout

When using `@task()` decorator, we add `timeout=5` to timeout in 5 seconds.

A solution was drafted in the branch:

<https://github.com/ansible/awx/compare/devel...AlanCoding:awx:dispatcher_timeout>

#### Singleton Tasks

AWX commonly used pg locks to prevent multiple workers running the same task,
but a more efficient alternative is to never start those tasks.

This proposes another argument to `@task()` decorator that makes the task exclusive.
When another version of the task is already running, there are 2 sub-options we could do:

- wait for the existing task to finish before running the new task
- discard the new task

The use cases for AWX mainly wand the 2nd one.
Idepotent tasks are used extremely heavily on schedules, meaning that
when the dispatcher receives too many it should simply discard extras.

#### Triggering Tasks from Tasks

For the solution to (2) in the cookbook to be fully functional,
it is best that tasks can directly start other tasks via messaging
internal to the worker pool.

This means passing some kind of object into the task being called
where this object contains callbacks that can be used to
trigger methods in the worker pool's finished watcher.
