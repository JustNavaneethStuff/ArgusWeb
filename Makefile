.PHONY: up down build migrate test test-unit test-integration logs shell-api

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

migrate:
	docker compose run --rm migrate

test:
	uv run pytest tests/ -v

test-unit:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v -m integration

logs:
	docker compose logs -f scheduler crawler parser cleaner api

shell-api:
	docker compose exec api bash

install:
	uv sync --all-packages

dev-api:
	cd services/api && uv run python -m api.main

dev-scheduler:
	cd services/scheduler && uv run python -m scheduler.main
