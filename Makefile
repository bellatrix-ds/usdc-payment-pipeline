.PHONY: up down test lint

up:
	docker compose up -d

down:
	docker compose down

test:
	pytest -q

lint:
	python -m compileall -q src tests
