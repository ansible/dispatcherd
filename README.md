# dispatcher
Working space for dispatcher prototyping

This is firstly intended to be a code split of:

https://github.com/ansible/awx/tree/devel/awx/main/dispatch

As a part of doing the split, we also want to resolve a number of
long-standing design and sustainability issues, thus, asyncio.

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
