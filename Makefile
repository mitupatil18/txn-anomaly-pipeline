.PHONY: up down logs test test-unit lint fmt migrate load benchmark scale replay-dlq

n ?= 3

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

test-unit:
	pytest tests/unit --cov --cov-fail-under=80

test:
	pytest --cov --cov-fail-under=80

lint:
	ruff check .
	ruff format --check .
	mypy src

fmt:
	ruff check --fix .
	ruff format .

migrate:
	docker compose run --rm migrate

load:
	docker compose --profile load run --rm simulator --rate 500 --duration 60 --accounts 2000 --fraud-ratio 0.02 --seed 42

benchmark:
	python scripts/benchmark.py --messages 200000 --runs 3 --experiments A B C D E

scale:
	docker compose up -d --scale consumer=$(n) consumer

replay-dlq:
	docker compose run --rm --entrypoint python consumer scripts/replay_dlq.py --limit 100
