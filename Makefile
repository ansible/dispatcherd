DOCKER_COMPOSE ?= docker compose
TEST_DIRS ?= tests/


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
	rm -rf dispatcher.egg-info/

linters:
	black dispatcher/
	isort dispatcher/
	flake8 dispatcher/
	mypy dispatcher

demo:
	docker compose up -d --wait

stop-demo:
	docker compose down

## Runs pytest synchronous and async tests in different processes
test:
	pytest $(TEST_DIRS)
	pytest $(TEST_DIRS) -m "asyncio"
