"""Deterministic identifiers for blockchain events."""

import hashlib


def make_event_id(tx_hash: str, log_index: int) -> str:
    return hashlib.sha256(f"{tx_hash}:{log_index}".encode()).hexdigest()
