# Worker Status Lifecycle

The `PoolWorker` (`dispatcherd/service/pool.py`) tracks a status field that drives both
scaling decisions and which workers may receive new tasks. The table below summarizes
each status, how it is reached, and whether it counts towards pool capacity or can
accept new work.

| Status        | Meaning / Transition                                                                                           | Counts for capacity?* | Eligible for task dispatch? |
|---------------|----------------------------------------------------------------------------------------------------------------|-----------------------|-----------------------------|
| `initialized` | Worker object exists but the subprocess has not been forked yet.                                               | ✅                    | ❌                          |
| `spawned`     | Subprocess has been forked (`start()`) but we have not yet begun the worker’s startup routine.                 | ✅                    | ❌                          |
| `starting`    | Worker is running its startup routine and will emit a `ready` event when it can accept work.                   | ✅                    | ❌                          |
| `ready`       | Worker sent a `ready` event (`read_results_forever`) and may receive tasks.                                     | ✅                    | ✅ (via `is_ready`)         |
| `stopping`    | A stop signal was queued (e.g., scale-down). Worker should finish current task and then exit.                  | ❌                    | ❌                          |
| `exited`      | Worker sent a `shutdown` event and the exit watcher is waiting for the process to terminate cleanly.           | ❌                    | ❌                          |
| `retired`     | `worker.stop()` confirmed the process is gone and the worker is only kept for stats until removal.             | ❌                    | ❌                          |
| `error`       | Worker died unexpectedly or failed to stop cleanly.                                                            | ❌                    | ❌                          |

`*` “Counts for capacity” refers to `PoolWorker.counts_for_capacity`, which is used by
scale-up/scale-down heuristics. The only status eligible for task dispatch is `ready`,
as enforced by `PoolWorker.is_ready` and `Queuer.get_free_worker()`.

These definitions ensure that once a worker transitions away from `ready`, no new
tasks will be dispatched to it, even if the management code releases its lock before
`worker.stop()` finishes the teardown sequence.
