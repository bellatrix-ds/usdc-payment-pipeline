"""Unit tests for Kafka message parsing and ClickHouse batch flushing."""

import json
from copy import deepcopy
from unittest.mock import MagicMock, patch

from src import consumer


def _event(event_id: str = "event-1") -> dict:
    return {
        "event_id": event_id,
        "tx_hash": "0xdeadbeef",
        "log_index": 1,
        "block_number": 20_000_000,
        "block_timestamp": 1_715_000_000,
        "from_address": "0xabc",
        "to_address": "0xdef",
        "amount_raw": 1_000_000,
        "ingested_at": "2024-05-06T12:00:00",
    }


def _message(value: bytes) -> MagicMock:
    message = MagicMock()
    message.value.return_value = value
    return message


def test_valid_json_message_is_parsed() -> None:
    raw = _event()
    message = _message(json.dumps(raw).encode())

    assert consumer.parse_message(message) == raw


def test_malformed_json_is_skipped_without_crashing() -> None:
    message = _message(b"{not-json")

    with patch.object(consumer.logger, "warning") as warning:
        assert consumer.parse_message(message) is None

    warning.assert_called_once()


def test_ten_messages_produce_ten_clickhouse_rows() -> None:
    client = MagicMock()
    kafka_consumer = MagicMock()
    buffer = [_event(f"event-{index}") for index in range(10)]

    inserted = consumer.flush(client, kafka_consumer, buffer)

    assert inserted == 10
    rows = client.insert.call_args.args[1]
    assert len(rows) == 10
    kafka_consumer.commit.assert_called_once_with(asynchronous=False)
    assert buffer == []


def test_duplicate_event_id_is_deduplicated_within_batch() -> None:
    client = MagicMock()
    kafka_consumer = MagicMock()
    duplicate = _event("duplicate")
    buffer = [duplicate, deepcopy(duplicate), _event("unique")]

    inserted = consumer.flush(client, kafka_consumer, buffer)

    assert inserted == 2
    rows = client.insert.call_args.args[1]
    event_id_index = consumer.COLUMN_NAMES.index("event_id")
    assert {row[event_id_index] for row in rows} == {"duplicate", "unique"}


def test_empty_batch_does_not_insert_or_commit() -> None:
    client = MagicMock()
    kafka_consumer = MagicMock()

    assert consumer.flush(client, kafka_consumer, []) == 0
    client.insert.assert_not_called()
    kafka_consumer.commit.assert_not_called()
