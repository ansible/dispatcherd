## Message Formats

There are two different types of message formats.

See the main design diagram for reference.

### Broker Message Format

This is the format when a client submits a task to be ran, for example, to pg_notify.
This contains JSON-serialized data.

Example:

```json
{
  "uuid": "9760671a-6261-45aa-881a-f66929ff9725",
  "args": [4],
  "kwargs": {},
  "task": "awx.main.tasks.jobs.RunJob",
  "time_pub": 1727354869.5126922,
  "guid": "8f887a0c51f7450db3542c501ba83756"
}
```

The `"task"` contains an importable task to run.

If you are doing the control-and-reply for something, then the submitted
message will also contain a `"reply_to"` key for the channel to send the reply to.

The message sent to the reply channel will have some other purpose-specific information,
like debug information.

### Internal Worker Pool Format

The main process and workers communicate through conventional IPC queues.
This contains the messages to start running a job, of course.
Ideally, this only contains the bare minimum, because tracking
stats and lifetime are the job of the main process, not the worker.

```json
{
  "args": [4],
  "kwargs": {},
  "task": "awx.main.tasks.jobs.RunJob",
}
```

#### Worker to Main Process

When the worker communicates information back to the main process for several reasons.

##### Ready-for-work message

After starting up, the worker sends this message to indicate that
it is ready to receive tasks.

```json
{
    "worker": 3,
    "event": "ready"
}
```

##### Finished-a-task message

Workers send messages via a shared queue, so one thing that
must be present is the `worker_id` identifier so that the main
process knows who its from.
Other information is given for various stats tracking.

```json
{
    "worker": 3,
    "event": "done",
    "result": null,
    "uuid": "9760671a-6261-45aa-881a-f66929ff9725",
    "time_started": 1744992973.5737305,
    "time_finish": 1744992980.0253727,
}
```

Most tasks are expected to give a `None` value for its return value.
This library does not support handling of results formally,
but result may be used for some testing function via logging.

##### Control-action message

Workers can use the IPC mechanism to perform control actions
if they have set `bind=True`. This allows bypassing the broker
which has performance and stability benefits.

The message to the parent looks like:

```json
{
    "worker": 3,
    "event": "control",
    "command": "running",
    "control_data": {},
}
```

##### Shutting down message

This is a fairly static method, but it is very important
for pool management, since getting this message indicates
to the parent the process can be `.join()`ed.

```json
{
    "worker": 3,
    "event": "shutdown",
}
```
