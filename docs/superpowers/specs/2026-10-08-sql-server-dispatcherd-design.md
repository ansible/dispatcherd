# SQL Server-backed dispatcherd broker

**Status:** Draft for review  
**Date:** 2026-10-08

## Context

Dispatcherd currently has a PostgreSQL `pg_notify` broker. The dispatcherd design notes explain that notifications are not a persistent queue, and its broker protocol already lets applications select a broker through configuration. Metrics-service currently uses that broker to transport task identifiers; its task records, state transitions, and reconciliation remain in its own database.

The local SQL Server POC implements a broker with `pyodbc` 5.3.0 and Microsoft ODBC Driver 18 for SQL Server. This first change moves metrics-service dispatch to SQL Server. Metrics-service's Django database migration is a separate follow-up; this broker must not derive its connection settings from Django's `DATABASES` setting.

## Goals

- Add a SQL Server broker to dispatcherd using the same driver stack as the POC: `pyodbc` and ODBC Driver 18.
- Store dispatch messages in a dedicated SQL Server database named `dispatcherd`.
- Route metrics-service's dispatcherd traffic through that broker.
- Keep broker selection in dispatcherd configuration so a PostgreSQL broker can remain available to other consumers and a future install-time choice can build on the same interface.
- Keep SQL Server connection settings independent from the metrics-service application database settings.
- Preserve the existing channel and JSON payload contract used by dispatcherd publishers and consumers.

## Non-goals

- Migrate metrics-service's Django database from PostgreSQL to SQL Server.
- Add a product-level install-time database selector across metrics-service and other components. Metrics-service will select SQL Server for this phase; its broker configuration will remain explicit and replaceable.
- Add SQL Server Service Broker wake-up objects. The first version will poll the message table.
- Implement leases, acknowledgements, retries, a dead-letter queue, or a generalized multi-database SQL abstraction.
- Remove the existing `pg_notify` broker from dispatcherd.

## Design

### Broker and driver

Add `dispatcherd.brokers.sql_server` with a `Broker` implementing dispatcherd's existing broker protocol. The module will use `pyodbc` and accept connection parameters through broker configuration, including server, port, database, user, password, ODBC driver, and certificate settings. The optional dependency will be exposed as a SQL Server extra so users of other brokers do not need `pyodbc`.

The Python dependency will be `pyodbc>=5.3.0`. The default ODBC driver name will be `ODBC Driver 18 for SQL Server`, matching the POC. Metrics-service container builds will install the matching Microsoft ODBC driver and unixODBC runtime. Credentials will come from runtime configuration and will not be committed in YAML or source files.

The broker will implement both synchronous publishing/receiving and asynchronous service publishing/receiving. Blocking ODBC work in asynchronous paths will run in an executor so polling does not block dispatcherd's event loop. `poll_interval_seconds` will control idle polling latency.

### Message table and delivery

The dedicated `dispatcherd` database will contain `dbo.dispatcherd_messages` with an identity key, channel, JSON payload, and UTC creation time. A publisher inserts one row per message. A consumer polls only its configured channels and atomically claims a batch by deleting the selected rows and returning them. SQL Server row locks will prevent two dispatcherd consumers from claiming the same row.

Claimed rows are removed before task execution, so delivery is at-most-once after a successful claim. A process failure between claim and worker execution can lose that broker message. Metrics-service's existing task-state reconciliation remains responsible for resubmitting pending work. An idempotent SQL setup script will create the dedicated database and message table. The broker itself will not create databases or enable SQL Server Service Broker.

### Metrics-service integration

Change `apps/settings/dispatcherd.yaml` to select the SQL Server broker and retain the existing metrics, dashboard, and maintenance channels. Add a separate `DISPATCHERD_SQLSERVER` metrics-service setting, populated from `METRICS_SERVICE_DISPATCHERD_SQLSERVER__*` environment configuration. `dispatcherd_config.py` will merge that setting into the broker config rather than injecting `DATABASES["default"]` connection values.

Remove `psycopg2-binary`, which metrics-service currently includes specifically for dispatcherd's PostgreSQL `LISTEN`/`NOTIFY`. Keep `psycopg` for the metrics-service Django database in this phase. Update metrics-service's production and development container builds to install ODBC Driver 18, and document the transition and required environment settings.

### Future install-time choice

Dispatcherd already selects its broker by configuration key, so its API will remain provider-neutral. This phase selects SQL Server for metrics-service without adding a cross-component install-time selector. A later change can expose PostgreSQL versus SQL Server as a deployment choice without coupling task publication to Django's database engine.

## Expected files

- `components/dispatcherd/dispatcherd/brokers/sql_server.py`
- `components/dispatcherd/pyproject.toml`, `schema.json`, and dispatcherd broker/configuration documentation
- `components/metrics-service/apps/settings/dispatcherd.yaml`
- `components/metrics-service/apps/settings/defaults.py` and `apps/tasks/dispatcherd_config.py`
- `components/metrics-service/pyproject.toml` and `uv.lock`
- `components/metrics-service/Dockerfile` and `Dockerfile.dev`
- Metrics-service task-system documentation and SQL Server setup instructions
- A dispatcherd SQL Server database setup script

## Design references

- `components/dispatcherd/docs/design_notes.md`
- `components/dispatcherd/docs/config.md`
- `components/dispatcherd/docs/message_formats.md`
- `components/dispatcherd/docs/submit_task.md`
- `components/metrics-service/docs/task-system.md`
- Local SQL Server POC connection settings: `pyodbc==5.3.0` and `ODBC Driver 18 for SQL Server`
