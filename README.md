# USDC Payment Pipeline

End-to-end streaming pipeline ingesting, deduplicating, modelling, and monitoring USDC Transfer events from Base chain.

**Status:** Under active development

## Stack

Python, Kafka, Flink, ClickHouse, dbt, Airflow, Grafana

## Architecture

```text
Alchemy RPC → Python Producer → Kafka → Flink → ClickHouse → dbt → Grafana
```

The producer backfills and streams ERC-20 Transfer logs from Base. Kafka decouples ingestion from processing, Flink applies event-time validation and idempotent deduplication, ClickHouse stores canonical events, dbt creates payment marts, Airflow coordinates backfills and quality checks, and Grafana exposes operational and payment metrics.

## Engineering goals

- Process at least two million USDC transfers reproducibly.
- Preserve block, transaction, and log-level lineage.
- Deduplicate by chain ID, transaction hash, and log index.
- Support safe replay, late arrivals, retries, and Base reorg handling.
- Monitor consumer lag, throughput, freshness, invalid records, and duplicate rates.
- Reconcile source logs against canonical and modeled event counts.

## Repository guide

| Path | Purpose |
|---|---|
| `src/` | RPC ingestion and streaming application code |
| `sql/clickhouse/` | ClickHouse DDL and operational queries |
| `dbt/` | Staging, intermediate, and payment mart models |
| `airflow/dags/` | Backfill, quality, and orchestration DAGs |
| `dashboards/grafana/` | Version-controlled dashboard definitions |
| `tests/` | Unit, integration, contract, and reconciliation tests |
| `docs/` | Architecture and data-model documentation |

## Local development

```bash
cp .env.example .env
make up
make test
```

Never commit `.env` or provider credentials.
