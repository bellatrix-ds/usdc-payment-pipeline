"""Unit tests for Transfer decoding and Kafka message construction."""

import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.common.abi import TRANSFER_TOPIC, decode_transfer_log
from src.common.idempotency import make_event_id
from src import producer


def _raw_log(topic0: str = TRANSFER_TOPIC) -> dict:
    return {
        "transactionHash": "0xdeadbeef",
        "logIndex": "0x1",
        "blockNumber": "0x1312d00",
        "blockTimestamp": "0x6640a1b0",
        "topics": [
            topic0,
            "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12",
            "0x000000000000000000000000fedcba0987654321fedcba0987654321fedcba09",
        ],
        "data": "0x00000000000000000000000000000000000000000000000000000000000f4240",
    }


def test_decode_transfer_log_parses_addresses_and_amount() -> None:
    decoded = decode_transfer_log(_raw_log())

    assert decoded is not None
    assert decoded["from_address"] == "0xabcdef1234567890abcdef1234567890abcdef12"
    assert decoded["to_address"] == "0xfedcba0987654321fedcba0987654321fedcba09"
    assert decoded["amount_raw"] == 1_000_000


def test_decode_transfer_log_rejects_non_transfer_event() -> None:
    assert decode_transfer_log(_raw_log("0x" + "0" * 64)) is None


def test_decoded_amount_is_integer_and_timestamp_is_present() -> None:
    decoded = decode_transfer_log(_raw_log())

    assert decoded is not None
    assert isinstance(decoded["amount_raw"], int)
    assert decoded["block_timestamp"] == int("6640a1b0", 16)


def test_publish_adds_event_id_to_decoded_log() -> None:
    kafka_producer = MagicMock()
    kafka_producer.flush.return_value = 0
    producer_class = MagicMock(return_value=kafka_producer)
    kafka_module = SimpleNamespace(Producer=producer_class)
    config = SimpleNamespace(
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        KAFKA_TOPIC="usdc.transfers",
    )

    with (
        patch.dict(sys.modules, {"confluent_kafka": kafka_module}),
        patch.object(producer, "_load_config", return_value=config),
    ):
        published = producer.publish_to_kafka([_raw_log()])

    assert published == 1
    payload = json.loads(kafka_producer.produce.call_args.kwargs["value"])
    assert payload["event_id"] == make_event_id("0xdeadbeef", 1)
    assert len(payload["event_id"]) == 64
