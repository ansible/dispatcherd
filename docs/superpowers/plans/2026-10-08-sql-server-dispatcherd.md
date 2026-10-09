# SQL Server-backed dispatcherd Broker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace metrics-service's `pg_notify` dispatch path with a SQL Server table broker while preserving dispatcherd's broker protocol and leaving its PostgreSQL broker available to other users.

**Architecture:** Add `dispatcherd.brokers.sql_server`, using `pyodbc` and ODBC Driver 18 to insert and atomically claim channel messages in the dedicated `dispatcherd` database. Configure metrics-service with an independent `DISPATCHERD_SQLSERVER` setting and install the ODBC runtime in its containers; keep its Django database on PostgreSQL in this phase.

**Tech Stack:** Python 3.11-3.14 in dispatcherd, Python 3.12 in metrics-service, pyodbc >=5.3.0, Microsoft ODBC Driver 18 for SQL Server, Django/Dynaconf, pytest.

**Spec:** [docs/superpowers/specs/2026-10-08-sql-server-dispatcherd-design.md](../specs/2026-10-08-sql-server-dispatcherd-design.md)

## Global Constraints

- Use `pyodbc>=5.3.0` and default to `ODBC Driver 18 for SQL Server`.
- Store broker rows in the dedicated SQL Server database named `dispatcherd`.
- Keep broker selection in dispatcherd configuration and keep the existing `pg_notify` broker available.
- Keep SQL Server broker configuration independent of metrics-service `DATABASES["default"]`.
- Keep metrics-service's Django database on PostgreSQL in this phase.
- Do not commit credentials; read connection values from runtime configuration.
- Provision the database and message table with an idempotent SQL setup script; do not create databases or enable SQL Server Service Broker from the runtime broker.
- Consume by atomically deleting claimed rows; delivery is at-most-once after a claim, and metrics-service reconciliation resubmits pending work.
- Do not add leases, acknowledgements, retries, a dead-letter queue, or a generalized SQL dialect abstraction.
- Do not add SQL Server Service Broker wake-up objects; use polling.
- Do not add a cross-component install-time PostgreSQL/SQL Server selector in this change.
- Until a dispatcherd release containing the `sql_server` extra is available, metrics-service keeps its generic package requirement, installs `pyodbc` directly, and uses the sibling dispatcherd checkout for local integration.

## Review Focus

These conditions need explicit coverage because the spec leaves implementation details open:

1. **ODBC connection values with semicolons, braces, or empty passwords:** produce a valid escaped connection string and never log the password.
2. **More than one subscribed channel or no channels:** build parameterized channel filters, and do not generate invalid `IN ()` SQL.
3. **Large or non-ASCII JSON payloads:** round-trip a payload above PostgreSQL's 8 KB notification size without chunking or corruption.
4. **Two consumers polling the same channels:** row locking makes a message appear in at most one consumer's results.
5. **ODBC calls during async polling, reply publishing, shutdown, and driver errors:** keep blocking work off the event loop, serialize use of each connection, and close owned connections.

---

## File Map

### `components/dispatcherd`

- Create `dispatcherd/brokers/sql_server.py`: SQL Server connection-string construction, table inserts, atomic batch dequeue, synchronous methods, asynchronous service methods, and connection lifecycle.
- Modify `pyproject.toml`: add the optional `sql_server` extra for `pyodbc>=5.3.0`.
- Modify `dispatcherd/factories.py`: include the SQL Server broker in generated broker schema without connecting to a server.
- Modify `schema.json`: regenerate using `python tools/gen_schema.py`.
- Create `tools/sql_server/setup.sql`: idempotently create database `dispatcherd`, table `dbo.dispatcherd_messages`, and its dequeue index.
- Modify `docs/config.md`, `README.md`, and `docs/message_formats.md`: explain SQL Server setup, broker config, and persisted message transport; retain PostgreSQL broker instructions.
- Create `tests/unit/brokers/test_sql_server.py`: connection-string, SQL parameterization, empty-channel, payload, and fake DB-API connection behavior.
- Create `asyncio_tests/integration/brokers/test_sql_server.py`: opt-in tests against SQL Server for publish/consume and concurrent claims.

### `components/metrics-service`

- Modify `apps/settings/defaults.py`: define `DISPATCHERD_SQLSERVER` separately from `DATABASES`.
- Modify `apps/settings/dispatcherd.yaml`: select `sql_server` and keep the existing channels.
- Modify `apps/tasks/dispatcherd_config.py`: inject `DISPATCHERD_SQLSERVER`, stop deriving broker credentials from `DATABASES["default"]`, and keep the existing task timeout merge.
- Modify `tests/unit/tasks/test_dispatcherd_config.py`: cover the independent SQL Server broker configuration and preserve timeout handling.
- Modify `pyproject.toml` and `uv.lock`: add `pyodbc>=5.3.0`, remove `psycopg2-binary`, and retain `psycopg[binary]` for Django. Keep the generic dispatcherd requirement until a package release includes `sql_server`; install the sibling checkout locally with its extra.
- Modify `Dockerfile` and `Dockerfile.dev`: install unixODBC and Microsoft ODBC Driver 18.
- Modify `docs/task-system.md` and add SQL Server connection/setup documentation.

## Implementation Tasks

### Task 1: Add the SQL Server broker's synchronous database operations

**Files:**
- Create: `components/dispatcherd/tests/unit/brokers/test_sql_server.py`
- Create: `components/dispatcherd/dispatcherd/brokers/sql_server.py`
- Modify: `components/dispatcherd/pyproject.toml`

**Interfaces:**
- `Broker(config, channels=(), default_publish_channel=None, poll_interval_seconds=0.25, sync_connection=None, async_connection=None, sync_connection_factory=None, async_connection_factory=None)` implements the portions of `dispatcherd.protocols.Broker` used by synchronous publishers and asynchronous service consumers.
- `Broker.publish_message(channel=None, message='') -> str` inserts the message and returns the selected channel.
- `Broker.process_notify(connected_callback=None, timeout=5.0, max_messages=1) -> Iterator[tuple[str, str]]` polls and yields `(channel, payload)` pairs.
- Configured `sync_connection_factory` and `async_connection_factory` callbacks receive the broker's connection config as keyword arguments and return pyodbc-compatible DB-API connections.

- [x] **Step 1: Write failing synchronous broker tests**

Write focused tests using a fake cursor/connection. Assert that `publish_message` inserts the exact channel and full payload with `?` parameters, that a missing explicit channel resolves using `default_publish_channel` or the sole configured channel, that multiple channels use placeholders rather than SQL interpolation, and that empty subscriptions return no rows without emitting `IN ()`.

Add an escaping test for `_build_connection_string` using a password with `;` and `}`; assert the value is quoted correctly and is absent from captured logs.

- [x] **Step 2: Run the focused tests and confirm the expected failures**

Run: `pytest tests/unit/brokers/test_sql_server.py -q` from `components/dispatcherd`.  
Expected: FAIL because `dispatcherd.brokers.sql_server` does not exist.

- [x] **Step 3: Implement the synchronous broker path**

Use a lazy `pyodbc` import inside the default connection factory so schema generation and non-SQL users can import dispatcherd without installing the optional extra. Build the ODBC connection string from `config` using `server`, `database`, `user`, `password`, `driver`, and `trust_server_certificate`; accept `port` by appending it to the server when supplied separately. Open connections with autocommit enabled, matching the POC.

Use fixed, validated schema/table identifiers and parameterized values. For polling, select the oldest matching rows under `UPDLOCK, READPAST, ROWLOCK`, then delete the selected CTE and return the deleted `id`, `channel`, and `payload` with `OUTPUT`. Sort returned rows by `id` before yielding because SQL Server does not guarantee `OUTPUT` order.

Add the `sql_server = ["pyodbc>=5.3.0"]` optional dependency extra.

- [x] **Step 4: Run the focused tests and confirm they pass**

Run: `pytest tests/unit/brokers/test_sql_server.py -q` from `components/dispatcherd`.  
Expected: PASS, including the review-focus cases for connection-string escaping, empty/multiple channels, and large Unicode payloads.

### Task 2: Implement asynchronous broker lifecycle and SQL Server provisioning

**Files:**
- Modify: `components/dispatcherd/tests/unit/brokers/test_sql_server.py`
- Create: `components/dispatcherd/asyncio_tests/integration/brokers/test_sql_server.py`
- Modify: `components/dispatcherd/dispatcherd/brokers/sql_server.py`
- Create: `components/dispatcherd/tools/sql_server/setup.sql`

**Interfaces:**
- `Broker.apublish_message(channel=None, origin=None, message='') -> None` inserts a message asynchronously.
- `Broker.aprocess_notify(connected_callback=None) -> AsyncGenerator[tuple[str, str], None]` continuously polls configured channels and yields messages.
- `Broker.aclose()` and `Broker.close()` close only connections the broker created.

- [x] **Step 1: Write failing async and concurrency tests**

Test async publish, callback invocation after listener setup, yielded channel/payload, reply publishing while the receive generator is paused, cancellation/close, and surfaced driver errors. Use a single-worker executor per async connection so the same pyodbc connection is never moved between threads or used concurrently. Assert all ODBC calls use that executor and event-loop waits use `asyncio.sleep(poll_interval_seconds)` when no row is available.

Add an opt-in integration test guarded by `DISPATCHERD_SQLSERVER_TEST_CONFIG`, a JSON object containing the connection values accepted by `Broker`. The test publishes a unique message, starts two consumers subscribed to that channel, and asserts exactly one receives it. Also round-trip a 9 KB UTF-8 payload.

- [x] **Step 2: Run the focused async tests and confirm they fail for missing methods**

Run: `pytest tests/unit/brokers/test_sql_server.py -q` from `components/dispatcherd`.  
Expected: FAIL on the missing async lifecycle methods.

- [x] **Step 3: Implement asynchronous publication, polling, and shutdown**

Run blocking cursor operations through a broker-owned single-worker executor. Do not hold a DB cursor while yielding a message. Poll in batches, sleep asynchronously only after an empty poll, call `connected_callback` once after the listener is ready, and preserve the broker protocol's callback/reply flow. During shutdown, stop polling, close the owned connection on its executor, and shut down the executor. Support injected sync and async connections without closing caller-owned objects.

Do not implement SQL Server Service Broker signaling, self-check probes, retry leases, or dead-letter handling in this first version.

- [x] **Step 4: Add the idempotent SQL Server setup script**

Create `dispatcherd/tools/sql_server/setup.sql` that creates database `dispatcherd` if absent, switches to it, creates `dbo.dispatcherd_messages` with `BIGINT IDENTITY` primary key, `NVARCHAR(200)` channel, `NVARCHAR(MAX)` payload, and `DATETIME2` UTC creation time if absent, and creates an index on `(channel, id)` if absent. The script must not enable Service Broker or set `TRUSTWORTHY`.

- [x] **Step 5: Run broker unit tests and the opt-in SQL Server integration tests**

Run: `pytest tests/unit/brokers/test_sql_server.py -q` and, with the local SQL Server setup and `DISPATCHERD_SQLSERVER_TEST_CONFIG` configured, `pytest asyncio_tests/integration/brokers/test_sql_server.py -q`.  
Expected: unit tests pass; integration tests prove one-consumer claim behavior, payload fidelity, and sync/async publication against SQL Server.

### Task 3: Register the broker in dispatcherd configuration and documentation

**Files:**
- Modify: `components/dispatcherd/dispatcherd/factories.py`
- Modify: `components/dispatcherd/tests/unit/test_config.py`
- Modify: `components/dispatcherd/schema.json`
- Modify: `components/dispatcherd/docs/config.md`
- Modify: `components/dispatcherd/docs/message_formats.md`
- Modify: `components/dispatcherd/README.md`

**Interfaces:**
- Broker key: `sql_server` resolves to `dispatcherd.brokers.sql_server.Broker` using the existing dynamic module loader.
- The configuration schema lists the SQL Server constructor arguments without connecting to SQL Server.

- [x] **Step 1: Add a schema test for the SQL Server broker**

Extend the schema test fixture so `generate_settings_schema()` includes the SQL Server broker constructor fields while retaining `pg_notify` and socket entries. Construct the broker with an empty, non-secret `config` object; schema generation must not import pyodbc at module import or open a connection.

- [x] **Step 2: Run the schema test and confirm it fails**

Run: `pytest tests/unit/test_config.py::test_schema_is_current -q` from `components/dispatcherd`.  
Expected: FAIL because the SQL Server broker is not in the generated schema.

- [x] **Step 3: Register SQL Server in schema generation and regenerate `schema.json`**

Add a SQL Server broker entry to the schema-generation broker list in `factories.py` with an empty, non-secret `config` object and no connection attempt. Run `python tools/gen_schema.py > schema.json` from `components/dispatcherd`.

- [x] **Step 4: Document installation and configuration**

Update `docs/config.md` with `pip install 'dispatcherd[sql_server]'`, ODBC Driver 18 requirements, a config example using `brokers.sql_server`, channel selection, and poll interval. Explain that publisher and worker use the same broker config. Update `docs/message_formats.md` to state that the JSON message contract is unchanged and SQL Server stores payloads as `NVARCHAR(MAX)`. Update README's install/demo text to distinguish PostgreSQL and SQL Server broker setup.

- [x] **Step 5: Run dispatcherd's config and schema tests**

Run: `pytest tests/unit/test_config.py tests/testing/test_debug_brokers.py -q` from `components/dispatcherd`.  
Expected: PASS with the generated schema matching `schema.json` and existing broker resolution intact.

### Task 4: Switch metrics-service dispatch configuration to SQL Server

**Files:**
- Modify: `components/metrics-service/apps/settings/defaults.py`
- Modify: `components/metrics-service/apps/settings/dispatcherd.yaml`
- Modify: `components/metrics-service/apps/tasks/dispatcherd_config.py`
- Modify: `components/metrics-service/tests/unit/tasks/test_dispatcherd_config.py`
- Modify: `components/metrics-service/docs/task-system.md`
- Create: `components/metrics-service/docs/dispatcherd-sql-server.md`

**Interfaces:**
- Django setting: `DISPATCHERD_SQLSERVER` dictionary with `server`, `port`, `database`, `user`, `password`, `driver`, and `trust_server_certificate` keys.
- Environment overrides use Dynaconf's `METRICS_SERVICE_DISPATCHERD_SQLSERVER__<KEY>` convention.
- `setup_dispatcherd_config()` merges this setting into the configured `sql_server` broker and continues to apply Django's `TASK_TIMEOUT`.
- `_load_config_with_dispatcherd_settings(config_file: Path) -> dict[str, Any]` merges YAML with the independent setting.
- `build_config_from_settings() -> dict[str, Any]` builds the SQL Server broker config when no YAML file exists.

- [x] **Step 1: Write failing metrics-service config tests**

Replace PostgreSQL broker-config tests with tests that prove broker config comes from `DISPATCHERD_SQLSERVER` even when `DATABASES["default"]` has different values. Cover both loading `dispatcherd.yaml` and the no-file fallback, preserve configured channels, and assert the default timeout still comes from Django settings. Assert logs include server/database but not password.

- [x] **Step 2: Run the focused config tests and confirm they fail**

Run: `pytest tests/unit/tasks/test_dispatcherd_config.py -q` from `components/metrics-service`.  
Expected: FAIL because config currently injects PostgreSQL values into `pg_notify`.

- [x] **Step 3: Add the independent setting and update dispatcher config building**

Define `DISPATCHERD_SQLSERVER` in `apps/settings/defaults.py` with database `dispatcherd`, port `1433`, driver `ODBC Driver 18 for SQL Server`, and empty server/user/password values to force runtime configuration. Keep connection and password fields out of YAML. Change the YAML broker key from `pg_notify` to `sql_server` and retain `dashboard`, `maintenance`, and `metrics` channels.

Update `_load_config_with_django_db` and `build_config_from_django_settings` to use `DISPATCHERD_SQLSERVER`, rename these helpers to reflect their source, and remove `_postgres_connection_config`. Preserve config-file override behavior and the timeout merge. Do not change Django's `DATABASES` setting.

- [x] **Step 4: Update task-system and connection setup documentation**

Update `docs/task-system.md` to show the dispatcherd SQL Server broker and explicitly distinguish its `dispatcherd` database from metrics-service's current PostgreSQL application database. In `docs/dispatcherd-sql-server.md`, document required `METRICS_SERVICE_DISPATCHERD_SQLSERVER__*` settings, local host versus container host configuration, the setup script, and the temporary PostgreSQL application database dependency.

- [x] **Step 5: Run the focused metrics-service config tests**

Run: `pytest tests/unit/tasks/test_dispatcherd_config.py -q` from `components/metrics-service`.  
Expected: PASS; no broker configuration is derived from `DATABASES["default"]`.

### Task 5: Add SQL Server runtime dependencies to metrics-service containers

**Files:**
- Modify: `components/metrics-service/pyproject.toml`
- Modify: `components/metrics-service/uv.lock`
- Modify: `components/metrics-service/Dockerfile`
- Modify: `components/metrics-service/Dockerfile.dev`

**Interfaces:**
- Metrics-service installs `pyodbc>=5.3.0` directly for the current package release; local integration replaces the PyPI dispatcherd install with the sibling checkout and its `sql_server` extra.
- The runtime image provides unixODBC and `msodbcsql18` so ODBC Driver 18 is available to pyodbc.

- [x] **Step 1: Change Python dependencies for the local source transition**

In `pyproject.toml`, keep `dispatcherd>=2026.1.27`, remove `psycopg2-binary>=2.9.0`, add `pyodbc>=5.3.0`, and retain `psycopg[binary]>=3.1` for Django. Regenerate `uv.lock` with `uv lock`. For the local metrics-service environment, install the updated dispatcherd checkout and its extra after syncing the environment: `uv pip install --python .venv/bin/python --editable '../dispatcherd[sql_server]'`. Once a dispatcherd release containing the extra is published, the direct pyodbc dependency can move to `dispatcherd[sql_server]`.

- [x] **Step 2: Add ODBC Driver 18 to production and development images**

In both Dockerfiles, add Microsoft's RHEL 9 repository, install `unixODBC` and `msodbcsql18` with the Microsoft EULA accepted, then clean package-manager caches. Follow the POC's UBI/RHEL 9 installation sequence. Preserve `postgresql-libs` in `Dockerfile.dev` until the separate Django database migration.

- [x] **Step 3: Verify dependency metadata and container configuration**

Run `uv lock --check` from `components/metrics-service`, inspect `uv.lock` to confirm it contains pyodbc and no psycopg2-binary, and review both Dockerfiles to confirm the ODBC driver is installed before the non-root runtime stage.

### Task 6: Verify metrics-service configuration and SQL Server transport

- [x] **Step 1: Provision the dedicated database**

Run `components/dispatcherd/tools/sql_server/setup.sql` against the local Podman SQL Server. The setup script must create database `dispatcherd` and `dbo.dispatcherd_messages` without enabling Service Broker. Keep credentials in the container's secret environment; do not put them in source or command output.

- [x] **Step 2: Verify metrics-service loads the SQL Server broker settings**

With the local dispatcherd checkout installed as described in Task 5, set `METRICS_SERVICE_MODE=test` and `METRICS_SERVICE_DISPATCHERD_SQLSERVER__SERVER`/`__USER` in the environment. Run `manage.py check`, then call `setup_dispatcherd_config()` and inspect only the broker name, server, database, and channels. The resulting broker key must be `sql_server`; the Django database settings must remain separate.

- [x] **Step 3: Verify live message delivery on SQL Server**

Run `asyncio_tests/integration/brokers/test_sql_server.py` from the temporary ODBC-enabled Podman test container. Expected: a large UTF-8 payload round-trips and concurrent consumers claim one message exactly once. This live broker verification is separate from metrics-service TaskExecution persistence.

- [x] **Step 4: Record the task-state verification boundary**

Check whether a PostgreSQL application database is configured locally before attempting the full `TaskExecution` state transition. None is configured here (the expected local ports are closed and no database environment settings are present), so the dispatch transport and config are verified while the PostgreSQL-backed state lifecycle remains outside this phase.

- [x] **Step 5: Review cross-repository diffs**

Run `git diff --check` in both `components/dispatcherd` and `components/metrics-service`. Confirm no credentials are in tracked files and that `pg_notify` remains available in dispatcherd while metrics-service selects `sql_server`.
