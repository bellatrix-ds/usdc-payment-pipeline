"""Tests for deterministic blockchain event identifiers."""

import string

from src.common.idempotency import make_event_id


def test_event_id_is_deterministic() -> None:
    assert make_event_id("0xabc123", 7) == make_event_id("0xabc123", 7)


def test_different_log_index_produces_different_event_id() -> None:
    assert make_event_id("0xabc123", 7) != make_event_id("0xabc123", 8)


def test_different_transaction_hash_produces_different_event_id() -> None:
    assert make_event_id("0xabc123", 7) != make_event_id("0xdef456", 7)


def test_event_id_is_64_character_hex_string() -> None:
    event_id = make_event_id("0xabc123", 7)

    assert len(event_id) == 64
    assert set(event_id) <= set(string.hexdigits.lower())


def test_event_id_contains_no_uppercase_letters() -> None:
    event_id = make_event_id("0xABCDEF", 99)

    assert event_id == event_id.lower()
