DOCKER_COMPOSE ?= docker compose


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

linters:
	black dispatcherd/
	isort dispatcherd/
	flake8 dispatcherd/
	mypy dispatcherd

demo:
	docker compose up -d --wait

stop-demo:
	docker compose down
