#!/usr/bin/env python3
"""Backfill 14 days of Base USDC transfers directly into ClickHouse."""

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import clickhouse_connect
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from src.common import config  # noqa: E402
from src.common.abi import TRANSFER_TOPIC, decode_transfer_log  # noqa: E402
from src.common.idempotency import make_event_id  # noqa: E402


BLOCKS_PER_BATCH = 5
INSERT_BATCH_SIZE = 5000
MAX_CONCURRENCY = 5
REQUESTS_PER_SECOND = 10
MAX_ATTEMPTS = 3
RPC_TIMEOUT_SECONDS = 60
FAILED_BATCHES_PATH = REPO_ROOT / "logs" / "failed_batches.txt"

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


class RateLimiter:
    """Serialize request starts at a maximum configured rate."""

    def __init__(self, requests_per_second: int) -> None:
        self._interval = 1.0 / requests_per_second
        self._lock = asyncio.Lock()
        self._next_request_at = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next_request_at - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_request_at = time.monotonic() + self._interval


def validate_config() -> None:
    rpc_url = config.QUICKNODE_BASE_RPC_URL.strip()
    if not rpc_url or "user will fill this in" in rpc_url:
        raise RuntimeError(
            "Set QUICKNODE_BASE_RPC_URL in .env before running the backfill"
        )


async def rpc_request(
    session: aiohttp.ClientSession,
    payload: dict | list[dict],
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
) -> Any:
    async with semaphore:
        await rate_limiter.wait()
        async with session.post(
            config.QUICKNODE_BASE_RPC_URL, json=payload
        ) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}: {body[:500]}")
            try:
                return json.loads(body)
            except json.JSONDecodeError as exc:
                raise RuntimeError("QuickNode returned invalid JSON") from exc


async def get_current_block(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
) -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": [],
    }
    response = await rpc_request(session, payload, semaphore, rate_limiter)
    if "error" in response:
        raise RuntimeError(f"QuickNode RPC error: {response['error']}")
    return int(response["result"], 16)


async def fetch_block_timestamps(
    session: aiohttp.ClientSession,
    block_numbers: set[int],
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
) -> dict[int, str]:
    if not block_numbers:
        return {}

    ordered_blocks = sorted(block_numbers)
    timestamps: dict[int, str] = {}
    for offset in range(0, len(ordered_blocks), 100):
        block_chunk = ordered_blocks[offset : offset + 100]
        payload = [
            {
                "jsonrpc": "2.0",
                "id": block_number,
                "method": "eth_getBlockByNumber",
                "params": [hex(block_number), False],
            }
            for block_number in block_chunk
        ]
        response = await rpc_request(session, payload, semaphore, rate_limiter)
        if not isinstance(response, list):
            raise RuntimeError("QuickNode block batch response is not a list")

        for item in response:
            if "error" in item:
                raise RuntimeError(f"QuickNode block RPC error: {item['error']}")
            block = item.get("result")
            if not block or "number" not in block or "timestamp" not in block:
                raise RuntimeError("QuickNode block response is incomplete")
            timestamps[int(block["number"], 16)] = block["timestamp"]

    missing = set(ordered_blocks) - timestamps.keys()
    if missing:
        raise RuntimeError(f"Missing timestamps for {len(missing)} block(s)")
    return timestamps


async def fetch_logs_batch(
    session: aiohttp.ClientSession,
    start_block: int,
    end_block: int,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
) -> list[dict]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [
            {
                "address": config.USDC_CONTRACT_ADDRESS,
                "fromBlock": hex(start_block),
                "toBlock": hex(end_block),
                "topics": [TRANSFER_TOPIC],
            }
        ],
    }
    response = await rpc_request(session, payload, semaphore, rate_limiter)
    if "error" in response:
        raise RuntimeError(f"QuickNode RPC error: {response['error']}")
    logs = response.get("result")
    if not isinstance(logs, list):
        raise RuntimeError("QuickNode log response is not a list")

    missing_timestamp_blocks = {
        int(log["blockNumber"], 16)
        for log in logs
        if not log.get("blockTimestamp")
    }
    timestamps = await fetch_block_timestamps(
        session,
        missing_timestamp_blocks,
        semaphore,
        rate_limiter,
    )
    for log in logs:
        if not log.get("blockTimestamp"):
            log["blockTimestamp"] = timestamps[int(log["blockNumber"], 16)]
    return logs


def log_failed_batch(start_block: int, end_block: int, error: Exception) -> None:
    FAILED_BATCHES_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat()
    with FAILED_BATCHES_PATH.open("a", encoding="utf-8") as handle:
        handle.write(
            f"{timestamp} blocks={start_block}-{end_block} error={error}\n"
        )


async def fetch_with_retries(
    session: aiohttp.ClientSession,
    start_block: int,
    end_block: int,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
) -> tuple[int, int, list[dict], str | None]:
    last_error: Exception | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            logs = await fetch_logs_batch(
                session,
                start_block,
                end_block,
                semaphore,
                rate_limiter,
            )
            return start_block, end_block, logs, None
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            last_error = exc
            if attempt < MAX_ATTEMPTS - 1:
                delay = 2**attempt
                print(
                    f"Retrying blocks {start_block}-{end_block} in {delay}s "
                    f"after: {exc}"
                )
                await asyncio.sleep(delay)

    assert last_error is not None
    log_failed_batch(start_block, end_block, last_error)
    return start_block, end_block, [], str(last_error)


def get_clickhouse_client() -> Any:
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="payments",
        username="default",
        password="",
    )


def decode_rows(logs: list[dict]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    ingested_at = datetime.utcnow()
    for raw_log in logs:
        try:
            decoded = decode_transfer_log(raw_log)
            rows.append(
                [
                    make_event_id(decoded["tx_hash"], decoded["log_index"]),
                    decoded["tx_hash"],
                    decoded["log_index"],
                    decoded["block_number"],
                    datetime.utcfromtimestamp(decoded["block_timestamp"]),
                    decoded["from_address"],
                    decoded["to_address"],
                    int(decoded["amount_raw"]),
                    ingested_at,
                ]
            )
        except (KeyError, TypeError, ValueError) as exc:
            print(f"Skipping malformed log: {exc}", file=sys.stderr)
    return rows


def flush_rows(client: Any, rows: list[list[Any]]) -> int:
    if not rows:
        return 0
    client.insert("raw_transfers", rows, column_names=COLUMN_NAMES)
    count = len(rows)
    rows.clear()
    return count


def populate_materialized_views_if_empty(client: Any) -> None:
    """Populate targets only when they did not receive the direct inserts."""
    hourly_count = client.query(
        "SELECT count() FROM mv_hourly_activity"
    ).first_row[0]
    if hourly_count == 0:
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

    daily_count = client.query(
        "SELECT count() FROM mv_daily_summary"
    ).first_row[0]
    if daily_count == 0:
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


async def backfill() -> tuple[int, list[tuple[int, int, str]]]:
    validate_config()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    rate_limiter = RateLimiter(REQUESTS_PER_SECOND)
    timeout = aiohttp.ClientTimeout(total=RPC_TIMEOUT_SECONDS)
    client = get_clickhouse_client()
    pending_rows: list[list[Any]] = []
    inserted_total = 0
    event_total = 0
    skipped: list[tuple[int, int, str]] = []
    started_at = time.monotonic()

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            current_block = await get_current_block(
                session, semaphore, rate_limiter
            )
            start_block = current_block - (14 * 24 * 3600 * 2)
            end_block = current_block - 1
            ranges = [
                (start, min(start + BLOCKS_PER_BATCH - 1, end_block))
                for start in range(
                    start_block, end_block + 1, BLOCKS_PER_BATCH
                )
            ]
            total_blocks = end_block - start_block + 1
            print(
                f"Backfilling {total_blocks:,} blocks in {len(ranges):,} "
                f"batches ({start_block}-{end_block})"
            )

            tasks = [
                asyncio.create_task(
                    fetch_with_retries(
                        session, start, end, semaphore, rate_limiter
                    )
                )
                for start, end in ranges
            ]
            completed_blocks = 0
            for task in asyncio.as_completed(tasks):
                batch_start, batch_end, logs, error = await task
                batch_blocks = batch_end - batch_start + 1
                completed_blocks += batch_blocks
                if error is not None:
                    skipped.append((batch_start, batch_end, error))
                    print(f"SKIPPED blocks {batch_start}-{batch_end}: {error}")
                else:
                    event_total += len(logs)
                    pending_rows.extend(decode_rows(logs))
                    while len(pending_rows) >= INSERT_BATCH_SIZE:
                        chunk = pending_rows[:INSERT_BATCH_SIZE]
                        inserted_total += flush_rows(client, chunk)
                        del pending_rows[:INSERT_BATCH_SIZE]
                    print(
                        f"Blocks {batch_start}-{batch_end} → {len(logs):,} "
                        f"events (total so far: {event_total:,})"
                    )

                elapsed = time.monotonic() - started_at
                progress = completed_blocks / total_blocks
                eta_seconds = elapsed / progress - elapsed if progress else 0
                print(
                    f"Progress: {progress * 100:.1f}% | "
                    f"ETA: {eta_seconds / 60:.0f} min"
                )

        inserted_total += flush_rows(client, pending_rows)
        populate_materialized_views_if_empty(client)
    finally:
        client.close()

    print(f"Inserted {inserted_total:,} rows into payments.raw_transfers")
    return inserted_total, skipped


async def main() -> None:
    try:
        _, skipped = await backfill()
        run_dbt()
    except Exception as exc:
        print(f"Backfill failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if skipped:
        print(f"Completed with {len(skipped)} skipped batch(es):")
        for start, end, error in skipped:
            print(f"  {start}-{end}: {error}")
        print(f"Details: {FAILED_BATCHES_PATH}")
    else:
        print("Completed with no skipped batches.")


if __name__ == "__main__":
    asyncio.run(main())
