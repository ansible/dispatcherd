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

#### Chunked Messages

Large submissions may exceed a broker's payload limit (pg_notify limits payloads to roughly 8KB).
Before a message is published, dispatcherd brokers call
`dispatcherd.chunking.split_message`, which produces one or more JSON envelopes.
Each chunk wraps a slice of the original JSON payload so that it can be reassembled on the other end.

Below is a complete set of chunks for a single payload that had to be split into two pieces.
Note that both envelopes share the same `message_id`, their `index` values are contiguous,
and `total` is `2` on each chunk so consumers know when reassembly is finished.

```json
{
  "__dispatcherd_chunk__": "v1",
  "message_id": "4e66bf05b4b14222be14817a5eb918b4",
  "index": 0,
  "total": 2,
  "payload": "{\"uuid\":\"9760671a-6261-45aa-881a-f66929ff9725\",\"args\":[4,3"
}
{
  "__dispatcherd_chunk__": "v1",
  "message_id": "4e66bf05b4b14222be14817a5eb918b4",
  "index": 1,
  "total": 2,
  "payload": ",2,1],\"kwargs\":{},\"task\":\"awx.main.tasks.jobs.RunJob\"}"
}
```

If a message is not over the limit, it is sent as-is without the chunk wrapper.

The chunk envelope establishes the contract for multipart messages:

- `__dispatcherd_chunk__` identifies the chunk protocol version (`v1`) and is required.
- `message_id` is a per-message identifier used to correlate all pieces.
- `index` starts at ``0`` and increases by ``1`` for every chunk.
- `total` is the total number of chunks (repeated on every piece).
- `payload` contains an escaped slice of the original JSON string, so consumers
  reassemble the string data (not decoded dicts) before deserializing the complete message.

Consumers **must** detect the marker and reassemble the original payload before trying to
interpret it as a task. The helper in `dispatcherd/chunking.py` provides both sides
of this protocol:

```python
from dispatcherd.chunking import ChunkAccumulator

accumulator = ChunkAccumulator()
is_chunk, completed_payload, message_id = accumulator.ingest_dict(received_dict)
if is_chunk and completed_payload:
    # completed_payload is the dict described in "Broker Message Format"
    handle_task(completed_payload)
```

`ChunkAccumulator` tracks in-flight `message_id`s, waits until every index from ``0`` through
``total - 1`` has arrived, and discards stale fragments after
`message_timeout_seconds` (configured via `chunk_message_timeout_seconds` on the service).
Control replies use the same chunk envelope when they are large, so all inbound broker
traffic can be fed through a single accumulator.

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
    "time_finish": 1744992980.0253727
}
```

Most tasks are expected to give a `None` value for its return value.
This library does not support handling of results formally,
but result may be used for some testing function via logging.

When a worker raises `DispatcherExit` (for example, because an application-level SIGTERM
handler decided to shut it down), the done message includes an extra boolean flag so
the pool manager knows not to dispatch more work:

```json
{
    "worker": 3,
    "event": "done",
    "result": "<exit>",
    "uuid": "9760671a-6261-45aa-881a-f66929ff9725",
    "time_started": 1744992973.5737305,
    "time_finish": 1744992980.0253727,
    "is_stopping": true
}
```

The pool manager marks the worker as `stopping` **before** clearing its `current_task`
so the worker will not be scheduled for more work and will proceed to exit gracefully.

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
