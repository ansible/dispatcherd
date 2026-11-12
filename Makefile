DOCKER_COMPOSE ?= docker compose
TEST_DIRS ?= tests
ASYNC_TEST_DIRS ?= asyncio_tests
LINT_ROOT ?= .


# Mostly copied from DAB
postgres:
	docker start dispatch_postgres || $(DOCKER_COMPOSE) up -d msg_postgres --quiet-pull --wait

## Stops the postgres container started with 'make postgres'
stop-postgres:
	echo "Killing dispatch_postgres container"
	$(DOCKER_COMPOSE) rm -fsv msg_postgres

clean:
	find . -type f -regex ".*\.py[co]$$" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf dispatcherd.egg-info/
	rm -rf dist/
	find . -mindepth 1 -type d -empty -delete

linters:
	black $(LINT_ROOT)
	isort $(LINT_ROOT)
	flake8 $(LINT_ROOT)
	mypy

demo:
	docker compose up -d --wait

stop-demo:
	docker compose down

## Runs pytest synchronous and async tests in different processes
test:
	pytest $(TEST_DIRS)
	pytest $(ASYNC_TEST_DIRS)

## Start SQL Server for local integration tests (export MSSQL_SA_PASSWORD first).
.PHONY: sqlserver stop-sqlserver test-sqlserver test-sqlserver-docker
sqlserver:
	@test -n "$$MSSQL_SA_PASSWORD" || (echo "Export MSSQL_SA_PASSWORD before starting SQL Server"; exit 1)
	$(DOCKER_COMPOSE) up -d --wait sqlserver

## Stop SQL Server, preserving its database volume.
stop-sqlserver:
	$(DOCKER_COMPOSE) stop sqlserver

## Use the local Python virtual environment and native Microsoft ODBC Driver 18.
test-sqlserver:
	@test -n "$$MSSQL_SA_PASSWORD" || (echo "Export MSSQL_SA_PASSWORD so integration tests do not skip"; exit 1)
	.venv/bin/python -c 'import django, mssql, pyodbc; assert "ODBC Driver 18 for SQL Server" in pyodbc.drivers(), "Install Microsoft ODBC Driver 18 or use make test-sqlserver-docker"'
	DISPATCHERD_TEST_BACKEND=sqlserver .venv/bin/pytest -q --confcutdir=tests/sqlserver tests/sqlserver
	DISPATCHERD_TEST_BACKEND=sqlserver .venv/bin/pytest -q -o asyncio_default_fixture_loop_scope=function tests/integration \
		asyncio_tests/integration/test_bind.py \
		asyncio_tests/integration/test_callable_queues.py \
		asyncio_tests/integration/test_main.py \
		asyncio_tests/integration/test_pool.py \
		asyncio_tests/integration/test_signal_handling.py \
		asyncio_tests/integration/test_disruptions.py

## Alternative: driver and Python virtual environment are provided in the image.
test-sqlserver-docker:
	@test -n "$$MSSQL_SA_PASSWORD" || (echo "Export MSSQL_SA_PASSWORD before running SQL Server tests"; exit 1)
	$(DOCKER_COMPOSE) --profile sqlserver run --build --rm sqlserver-tests
