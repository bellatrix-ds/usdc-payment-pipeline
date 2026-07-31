# Architecture

USDC Transfer logs are read from Base through Alchemy RPC and published to Kafka with a deterministic key composed from chain ID, transaction hash, and log index. Flink validates event time, normalizes records, deduplicates replayed logs, and writes canonical events to ClickHouse. dbt builds payment-facing facts and aggregates; Airflow controls historical ranges and quality gates; Grafana monitors the system.

Key design concerns are RPC range management, partition ordering, checkpoint recovery, late events, reorg corrections, idempotent ClickHouse writes, and source-to-target count reconciliation.
