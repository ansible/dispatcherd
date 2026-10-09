DOCKER_COMPOSE ?= docker compose
TEST_DIRS ?= tests
ASYNC_TEST_DIRS ?= asyncio_tests
LINT_ROOT ?= .
MS_DEMO_COMPOSE = docker compose --env-file .env.ms-demo -f docker-compose-ms-demo.yml


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
	docker compose up --build -d --wait

stop-demo:
	docker compose down

ms-demo:
	@test -f .env.ms-demo || { echo "Create .env.ms-demo from .env.ms-demo.example and set the external SQL Server connection values"; exit 1; }
	@set -e; \
	demo_run_id=$$(python3 -c 'import uuid; print(uuid.uuid4().hex)'); \
	export MS_DEMO_RUN_ID=$$demo_run_id; \
	export COMPOSE_PROJECT_NAME=dispatcherd-ms-demo-$$demo_run_id; \
	echo "SQL Server demo run ID: $$demo_run_id"; \
	trap 'demo_status=$$?; echo "Stopping SQL Server demo containers..."; $(MS_DEMO_COMPOSE) down --remove-orphans || demo_status=$$?; exit $$demo_status' EXIT; \
	$(MS_DEMO_COMPOSE) up --build --detach --wait; \
	echo "Connecting to dispatcherd container 1 (node1) to send the task..."; \
	$(MS_DEMO_COMPOSE) exec --no-TTY ms_dispatcherd1 \
		/dispatcherd/venv/bin/python -m tools.demo.ms_sql_demo client

stop-ms-demo:
	@set -e; \
	demo_run_id=$${MS_DEMO_RUN_ID:-cleanup}; \
	export MS_DEMO_RUN_ID=$$demo_run_id; \
	export COMPOSE_PROJECT_NAME=dispatcherd-ms-demo-$$demo_run_id; \
	$(MS_DEMO_COMPOSE) down --remove-orphans

## Runs pytest synchronous and async tests in different processes
test:
	pytest $(TEST_DIRS)
	pytest $(ASYNC_TEST_DIRS)
