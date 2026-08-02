"""Consume USDC Transfer events from Kafka and write them to ClickHouse."""

import json
import logging
import signal
import time
from datetime import datetime
from typing import Any


BATCH_SIZE = 1000
FLUSH_INTERVAL = 5.0
GROUP_ID = "usdc-consumer-group"

COLUMN_NAMES = [
    "event_id",
    "tx_hash",
    "log_index",
    "block_number",
    "block_timestamp",
    "from_address",
    "to_address",
    "amount_raw",
    "ingested_at",
]

logger = logging.getLogger(__name__)


def _load_config() -> Any:
    try:
        from src.common import config
    except ModuleNotFoundError:
        from common import config
    return config


def get_clickhouse_client() -> Any:
    """Create and return a ClickHouse client using environment configuration."""
    import clickhouse_connect

    config = _load_config()
    try:
        return clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            database=config.CLICKHOUSE_DATABASE,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
        )
    except Exception as exc:
        raise ConnectionError(f"Unable to connect to ClickHouse: {exc}") from exc


def get_kafka_consumer() -> Any:
    """Create a manually committed Kafka consumer and subscribe to the topic."""
    from confluent_kafka import Consumer

    config = _load_config()
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([config.KAFKA_TOPIC])
        return consumer
    except Exception as exc:
        raise ConnectionError(f"Unable to create Kafka consumer: {exc}") from exc


def parse_message(msg: Any) -> dict | None:
    """Deserialize a Kafka message, rejecting malformed or incomplete values."""
    try:
        parsed = json.loads(msg.value())
        if not isinstance(parsed, dict):
            raise ValueError("message value must be a JSON object")
        missing = [field for field in COLUMN_NAMES if field not in parsed]
        if missing:
            raise ValueError(f"missing required fields: {', '.join(missing)}")
        return parsed
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError, ValueError) as exc:
        logger.warning("Skipping invalid Kafka message: %s", exc)
        return None


def buffer_to_rows(buffer: list[dict]) -> tuple[list, list]:
    """Convert buffered event dictionaries to ClickHouse column-oriented rows."""
    rows = []
    for message in buffer:
        converted = dict(message)
        converted["block_timestamp"] = datetime.utcfromtimestamp(
            int(message["block_timestamp"])
        )
        converted["ingested_at"] = datetime.fromisoformat(message["ingested_at"])
        converted["amount_raw"] = int(message["amount_raw"])
        rows.append([converted[column] for column in COLUMN_NAMES])
    return list(COLUMN_NAMES), rows


def flush(client: Any, consumer: Any, buffer: list[dict]) -> int:
    """Insert a buffered batch and commit Kafka offsets only after success."""
    if not buffer:
        return 0

    unique_messages = list(
        {message["event_id"]: message for message in buffer}.values()
    )
    column_names, rows = buffer_to_rows(unique_messages)
    try:
        client.insert("raw_transfers", rows, column_names)
        consumer.commit(asynchronous=False)
    except Exception:
        logger.exception("Failed to flush messages to ClickHouse")
        raise

    count = len(rows)
    buffer.clear()
    return count


def _handle_sigterm(_signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt


def run() -> None:
    """Run the consumer until interrupted, flushing by size or elapsed time."""
    signal.signal(signal.SIGTERM, _handle_sigterm)
    client = get_clickhouse_client()
    consumer = get_kafka_consumer()
    buffer: list[dict] = []
    last_flush_time = time.time()

    print("Consumer started. Waiting for messages...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.time()

            if msg is None:
                if now - last_flush_time >= FLUSH_INTERVAL:
                    count = flush(client, consumer, buffer)
                    if count:
                        print(f"Flushed {count} rows to ClickHouse")
                    last_flush_time = time.time()
                continue

            if msg.error():
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            parsed = parse_message(msg)
            if parsed is None:
                continue
            buffer.append(parsed)

            if (
                len(buffer) >= BATCH_SIZE
                or now - last_flush_time >= FLUSH_INTERVAL
            ):
                count = flush(client, consumer, buffer)
                print(f"Flushed {count} rows to ClickHouse")
                last_flush_time = time.time()
    except KeyboardInterrupt:
        print("Shutting down — flushing remaining buffer...")
        try:
            flush(client, consumer, buffer)
        finally:
            consumer.close()
        print("Consumer stopped cleanly.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
