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

When the worker communicates information back to the main process,
it must identify itself, and identify the event. For example:

```json
{
    "worker": 3,
    "event": "ready"
}
```
