# Worker Signal Handling Guide

The dispatcher uses several Unix signals internally, and external infrastructure
(systemd, Kubernetes, supervisors) adds its own behavior, so understanding the
interplay is relevant to app developers embeding dispatcherd.

## Signals Dispatcherd Uses

- **SIGUSR1** – reserved for dispatcher-driven task cancellation. The pool manager sends
  this to an in-flight worker when a timeout or cancel or manual cancel occurs. The worker raises
  `DispatcherCancel`, so user code must let that exception propagate if it wants cancel requests
  to succeed while keeping the worker alive for future tasks.
- **SIGINT** and **SIGTERM** – default python behavior while tasks are running.
  Idle workers process these signals and exit, which is necessary when the signal
  is given to the entire process group for shutdown.

### DispatcherCancel vs DispatcherExit

- `DispatcherCancel` tells the current task to stop but the worker stays alive and
  ready for more work. Let the exception bubble out (or re-raise it) so the dispatcher can
  record completion and reuse the worker.
- `DispatcherExit` is available for app authors who intercept SIGTERM/SIGINT themselves and
  determine the worker should shut down entirely. Raising `DispatcherExit` short-circuits
  the worker loop, sends the normal `event: "done"` message to its parent, but with `is_stopping: true`,
  then calls its shutdown hooks, does not request more work, and returns.

## Common Scenarios

1. **Supervisor / Kubernetes rolling update**
   - Typically sends SIGTERM to the entire process group, followed by SIGKILL if the
     app does not exit in time.
   - Idle workers will obey SIGTERM/SIGINT, report status to their parent, and exit.
   - Tasks must ensure that SIGTERM while running performs an orderly shutdown or
     allows the default `SystemExit`. This is up to the app using dispatcherd.

2. **Something runs `kill -TERM <worker-pid>`**
   - This bypasses the dispatcher’s normal shutdown path. An idling worker exits on
     the next message loop iteration. A running task should trap the signal only if
     it can ensure cleanup; otherwise the default handler ends the process and the
     manager will log an unexpected exit.

3. **Dispatcher cancel/timeout**
   - Comes through SIGUSR1 exclusively. User code should either catch
     `DispatcherCancel`, clean up, and re-raise, or let it bubble out naturally.
     Swallowing the exception means the manager will believe the task is still running
     and may resort to terminate/kill.

## Recommendations for Task Authors

- **Configure SIGINT/SIGTERM when your task starts.** Use your task’s `pre_task` hook (or
  equivalent entrypoint) to set application-specific handlers. Dispatcherd restores default
  handlers just before `pre_task` runs and reinstalls idle-mode handlers after the task finishes.
- **Never touch SIGUSR1.** The dispatcher depends on it for cancels/timeouts; overriding it
  breaks task cancel semantics.
- **Use `DispatcherCancel` for timeouts.** Treat it like any other exception that
  signals “we are done, begin cleanup”.
- **Raise `DispatcherExit` only when the worker must shut down.** This gives you precise control
  when your task owns SIGTERM handling (for example, reacting to Kubernetes pod termination).
- **Avoid `os.setsid()` or re-parenting tricks.** The dispatcher expects all workers
  to remain in the same group so supervisor signals reach everyone.
- **Log on shutdown paths.** When handling SIGTERM/SIGINT, emit concise logs so
  operators can correlate what happened during a rollout or incident.

### Example: configure `pre_task` via worker config

```python
# myapp/worker.py
import signal
from dispatcherd.worker.task import TaskWorker
from dispatcherd.worker.exceptions import DispatcherExit

class MyWorker(TaskWorker):
    def pre_task(self, message):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        signal.signal(signal.SIGINT, self._sigint_handler)
        super().pre_task(message)

    def _sigterm_handler(self, signum, frame):
        # stop the task and request worker shutdown
        raise DispatcherExit()

    def _sigint_handler(self, signum, frame):
        raise DispatcherExit()
```

```yaml
# dispatcher.yml
worker:
  worker_cls: "myapp.worker.MyWorker"
  worker_kwargs: {}
```
