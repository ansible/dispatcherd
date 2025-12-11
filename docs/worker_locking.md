# Worker Pool Locking Model

The worker pool uses three complementary primitives to keep task dispatch and
worker lifecycle transitions deterministic and concurrency-safe:

## 1. Worker registry lock (`WorkerData.management_lock`)

* Protects structural mutations to the worker registry (a standard `dict` keyed
  by worker id).
* Held only while adding/removing workers, or when taking a snapshot of the
  registry (e.g., for logging or health checks).
* Never used for per-worker state like `status` or `current_task`.
* Helper methods such as `snapshot()`, `busy_count()`, and the `all_*` queries
  encapsulate the lock acquisition so callers do not touch the registry
  directly.

## 2. Per-worker locks (`PoolWorker.lock`)

* Each `PoolWorker` owns an `asyncio.Lock`.
* Any read/write of mutable state (`status`, `current_task`, timers, cancel
  flags, etc.) must happen while the worker lock is held.
* Callers should never hold more than one worker lock at a time; if a worker
  lock is held, the registry lock must **not** be acquired to avoid deadlocks.

## 3. Ready queue (`WorkerData.ready_queue`)

* `collections.deque[int]` containing worker IDs that are eligible to run a task.
* Writers enqueue a worker only when it transitions into the `(status ==
  "ready", current_task is None)` state while holding the worker lock.
* `reserve_ready_worker(message)` pops the highest-priority worker (according to
  the rotation order), rechecks readiness under the worker lock, calls
  `worker.start_task(message)`, and returns the worker already marked busy.
  Stale queue entries are discarded automatically.

## Scheduling workflow

1. Task dispatch calls `reserve_ready_worker`. If it returns a worker, the pool
   logs the dispatch and continues. If no worker is available, the task is
   queued and the autoscaler is nudged.
2. When a worker sends a `ready` event or finishes a task (`done` event),
   `WorkerPool` clears `current_task`, resets bookkeeping, and enqueues the
   worker back into `ready_queue`.
3. Scale-down logic transitions idle workers to `"stopping"` under the worker
   lock, so any ready-queue entries are ignored when later popped.

## Lifecycle guarantees

* Worker exit: once a worker reports `'shutdown'`, its lock is held to mark it
  `'exited'` and prevent further dispatches. Registry removal happens later
  under the registry lock.
* Autoscale scale-down: requests a stop only when a worker is idle in `'ready'`
  state; `'stopping'` workers may temporarily linger in the ready queue but are
  skipped during reservation.
* Counting running tasks: tallies are derived on demand via `busy_count()`
  instead of being tracked incrementally.

Following these rules keeps locking localized, eliminates global scans to find
free workers, and preserves the deterministic worker rotation promise.
