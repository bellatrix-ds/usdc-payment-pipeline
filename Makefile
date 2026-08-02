.PHONY: start stop restart logs init-kafka init-clickhouse ingest-latest backfill-14days backfill-covalent generate-historical dbt-run dbt-test test lint

# ── Stack lifecycle ────────────────────────────────────────────────────────

start:
	docker compose up -d --build
	@echo "Waiting for services to be healthy (30s)..."
	@sleep 30
	@$(MAKE) init-kafka
	@echo ""
	@echo "Stack is up:"
	@echo "  Kafka (Redpanda)  http://localhost:9644"
	@echo "  ClickHouse        http://localhost:8123"
	@echo "  Airflow           http://localhost:8080  (admin / admin)"
	@echo "  Grafana           http://localhost:3000  (admin / admin)"

stop:
	docker compose down

restart: stop start

logs:
	docker compose logs -f

# ── One-time infra setup (idempotent) ─────────────────────────────────────

init-kafka:
	@echo "Creating Kafka topic: usdc.transfers (6 partitions)..."
	@docker compose exec -T redpanda rpk topic create usdc.transfers \
		--partitions 6 --replicas 1 2>/dev/null \
		&& echo "  Topic created." || echo "  Topic already exists."

init-clickhouse:
	@echo "Applying ClickHouse schemas..."
	@cat sql/clickhouse/001_raw_transfers.sql | \
		docker compose exec -T clickhouse clickhouse-client --database payments --multiquery
	@cat sql/clickhouse/002_fct_transfers.sql | \
		docker compose exec -T clickhouse clickhouse-client --database payments --multiquery
	@cat sql/clickhouse/003_materialized_views.sql | \
		docker compose exec -T clickhouse clickhouse-client --database payments --multiquery
	@echo "  Schemas applied."

ingest-latest:
	bash scripts/ingest_latest.sh

backfill-14days:
	python scripts/backfill_14days.py

backfill-covalent:
	python scripts/backfill_covalent.py

generate-historical:
	python scripts/generate_historical.py

# ── dbt ───────────────────────────────────────────────────────────────────

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

# ── Tests & lint ──────────────────────────────────────────────────────────

test:
	pytest tests/ -v

lint:
	python -m compileall -q src tests
