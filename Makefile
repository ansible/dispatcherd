DOCKER_COMPOSE ?= docker compose
TEST_DIRS ?= tests/
ASYNC_TEST_DIRS ?= asyncio_tests/


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
	black dispatcherd/ tests/ asyncio_tests/
	isort dispatcherd/ tests/ asyncio_tests/
	flake8 dispatcherd/ tests/ asyncio_tests/
	mypy dispatcherd tests asyncio_tests

demo:
	docker compose up -d --wait

stop-demo:
	docker compose down

## Runs pytest synchronous and async tests in different processes
test:
	pytest $(TEST_DIRS)
	pytest $(ASYNC_TEST_DIRS)
