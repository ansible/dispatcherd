# Broker parity integration tests

`asyncio_tests/integration/brokers/test_broker_contract.py` defines the shared
behavior expected of database brokers. The tests connect to a real PostgreSQL
server; they do not mock broker or database calls. Start PostgreSQL with
`make postgres`, then run:

```shell
pytest asyncio_tests/integration/brokers/test_broker_contract.py -v
```

The file is also included in the normal `make test` and CI test runs. Each case
is parameterized by backend through the `broker_backend` fixture. The default
run uses PostgreSQL only.

To also test SQL Server, use a checkout that includes the SQL Server broker,
install its optional dependencies, and provision a real SQL Server database with
the broker's schema. Set `DISPATCHERD_TEST_SQLSERVER_DSN` to its ODBC connection
string before running the same command. Both backends will then run every case;
pytest reports the backend in each test ID. The DSN can contain credentials, so
keep it out of shell history and logs. The test fixture redacts its representation
in pytest failures.

Other database brokers can be added to the fixture without changing the test
bodies. Provisioning and connection settings are backend-specific.

The contract covers synchronous and asynchronous publication and consumption,
channel selection and isolation, ordering and duplicate payloads, fan-out,
subscription readiness and cleanup, publishing replies while listening,
timeouts, restarting after close, and large Unicode messages. It also exercises
actual dispatcher task execution, delayed task cancellation, and synchronous and
asynchronous control replies, including large requests and replies. The offline
message test records PostgreSQL's current non-durable behavior: a publication
before a listener subscribes is not delivered to that listener.

This is a parity baseline, not a guarantee that every dispatcher feature has a
backend-neutral test. The existing PostgreSQL-specific dispatcher integration
suite covers additional task scheduling, duplication policies, worker behavior,
and connection disruptions. Backend-specific channel limits, health checks, and
connection failure/recovery should be tested separately, and additional common
cases should be added here as their cross-backend semantics are defined.

The SQL Server Docker runner also runs these shared cases, selecting only the
SQL Server backend. Export `MSSQL_SA_PASSWORD` and run
`make test-sqlserver-docker`; the image supplies a Python virtual environment and
the ODBC driver. No PostgreSQL server is needed for this invocation. The SQL Server
CI job runs the same command.
