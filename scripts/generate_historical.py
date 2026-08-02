#!/usr/bin/env python3
"""Generate 14 days of realistic synthetic USDC transfer history."""

import hashlib
import math
import random
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from src.common.idempotency import make_event_id  # noqa: E402


DAYS = 14
INSERT_BATCH_SIZE = 5000
START_BLOCK = 47_000_000
RANDOM_SEED = 8453
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

AMOUNT_BUCKETS = (
    (0.55, 1, 100),
    (0.20, 100, 1_000),
    (0.12, 1_000, 10_000),
    (0.08, 10_000, 100_000),
    (0.04, 100_000, 1_000_000),
    (0.01, 1_000_000, 5_000_000),
)


def make_address(rng: random.Random) -> str:
    return f"0x{rng.getrandbits(160):040x}"


def transfers_for_hour(timestamp: datetime, rng: random.Random) -> int:
    if timestamp.weekday() >= 5:
        low, high = 50, 150
    elif 9 <= timestamp.hour <= 11:
        low, high = 300, 500
    elif 14 <= timestamp.hour <= 16:
        low, high = 250, 400
    else:
        low, high = 80, 200
    return round(rng.triangular(low, high, high))


def generate_amount_raw(rng: random.Random) -> int:
    selection = rng.random()
    cumulative = 0.0
    minimum = maximum = 0
    for weight, bucket_minimum, bucket_maximum in AMOUNT_BUCKETS:
        cumulative += weight
        if selection <= cumulative:
            minimum, maximum = bucket_minimum, bucket_maximum
            break

    amount_usdc = 10 ** rng.uniform(math.log10(minimum), math.log10(maximum))
    return max(1_000_000, round(amount_usdc * 1_000_000))


def get_clickhouse_client() -> Any:
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="payments",
        username="default",
        password="",
    )


def flush_rows(client: Any, rows: list[list[Any]]) -> int:
    if not rows:
        return 0
    client.insert("raw_transfers", rows, column_names=COLUMN_NAMES)
    count = len(rows)
    rows.clear()
    return count


def populate_materialized_views_if_empty(client: Any) -> None:
    """Backfill targets only if active materialized views captured no inserts."""
    if client.query("SELECT count() FROM mv_hourly_activity").first_row[0] == 0:
        client.command(
            """
            INSERT INTO mv_hourly_activity
                (hour, transfer_count, volume_raw, unique_senders)
            SELECT
                toStartOfHour(block_timestamp),
                count(),
                sum(amount_raw),
                uniq(from_address)
            FROM raw_transfers
            GROUP BY toStartOfHour(block_timestamp)
            """
        )

    if client.query("SELECT count() FROM mv_daily_summary").first_row[0] == 0:
        client.command(
            """
            INSERT INTO mv_daily_summary
                (day, transfer_count, volume_raw, unique_senders, avg_amount_raw)
            SELECT
                toDate(block_timestamp),
                count(),
                sum(amount_raw),
                uniq(from_address),
                avg(toFloat64(amount_raw))
            FROM raw_transfers
            GROUP BY toDate(block_timestamp)
            """
        )


def run_dbt() -> None:
    subprocess.run(["dbt", "run"], cwd=REPO_ROOT / "dbt", check=True)


def generate_historical() -> int:
    rng = random.Random(RANDOM_SEED)
    from_addresses = [make_address(rng) for _ in range(100)]
    to_addresses = [make_address(rng) for _ in range(300)]
    now = datetime.utcnow()
    first_hour = (now - timedelta(days=DAYS)).replace(
        minute=0, second=0, microsecond=0
    )
    final_hour = now.replace(minute=0, second=0, microsecond=0)
    ingested_at = now
    block_number = START_BLOCK
    transfer_index = 0
    running_total = 0
    client = get_clickhouse_client()

    try:
        current_day = first_hour.date()
        while current_day <= final_hour.date():
            day_rows: list[list[Any]] = []
            hour = max(first_hour, datetime.combine(current_day, datetime.min.time()))
            day_end = min(
                final_hour,
                datetime.combine(current_day, datetime.max.time()).replace(
                    microsecond=0
                ),
            )

            while hour <= day_end:
                transfer_count = transfers_for_hour(hour, rng)
                for _ in range(transfer_count):
                    log_index = rng.randint(0, 10)
                    block_timestamp = hour + timedelta(
                        seconds=rng.randint(0, 3599)
                    )
                    tx_digest = hashlib.sha256(
                        f"{block_number}{transfer_index}".encode()
                    ).hexdigest()
                    tx_hash = f"0x{tx_digest}"
                    day_rows.append(
                        [
                            make_event_id(tx_hash, log_index),
                            tx_hash,
                            log_index,
                            block_number,
                            block_timestamp,
                            rng.choice(from_addresses),
                            rng.choice(to_addresses),
                            generate_amount_raw(rng),
                            ingested_at,
                        ]
                    )
                    transfer_index += 1
                    block_number += rng.randint(1, 3)
                hour += timedelta(hours=1)

            inserted_for_day = 0
            while day_rows:
                batch = day_rows[:INSERT_BATCH_SIZE]
                inserted_for_day += flush_rows(client, batch)
                del day_rows[:INSERT_BATCH_SIZE]
            running_total += inserted_for_day
            print(
                f"Inserted {inserted_for_day:,} rows for {current_day.isoformat()} "
                f"(running total: {running_total:,})"
            )
            current_day += timedelta(days=1)

        populate_materialized_views_if_empty(client)
    finally:
        client.close()

    print(f"Generated and inserted {running_total:,} historical transfers")
    return running_total


def main() -> None:
    try:
        generate_historical()
        run_dbt()
    except Exception as exc:
        print(f"Historical generation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print("Historical data generation completed successfully.")


if __name__ == "__main__":
    main()
