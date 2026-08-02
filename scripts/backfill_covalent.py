#!/usr/bin/env python3
"""Backfill seven days of Base USDC transfers with the GoldRush API."""

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import clickhouse_connect
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from src.common.abi import TRANSFER_TOPIC, decode_transfer_log  # noqa: E402
from src.common.idempotency import make_event_id  # noqa: E402


COVALENT_API_URL = (
    "https://api.covalenthq.com/v1/base-mainnet/events/address/"
    "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913/"
)
PAGE_SIZE = 1000
INSERT_BATCH_SIZE = 5000
BLOCK_CHUNK_SIZE = 900_000
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


def get_api_key() -> str:
    api_key = os.getenv("COVALENT_API_KEY", "").strip()
    if not api_key or "user will fill in" in api_key:
        raise RuntimeError("Set COVALENT_API_KEY in .env before running")
    return api_key


def get_quicknode_url() -> str:
    rpc_url = os.getenv("QUICKNODE_BASE_RPC_URL", "").strip()
    if not rpc_url or "user will fill this in" in rpc_url:
        raise RuntimeError("Set QUICKNODE_BASE_RPC_URL in .env before running")
    return rpc_url


def get_clickhouse_client() -> Any:
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        database="payments",
        username="default",
        password="",
    )


def parse_block_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def covalent_to_rpc_log(event: dict[str, Any]) -> dict[str, Any]:
    block_timestamp = parse_block_timestamp(event["block_signed_at"])
    unix_timestamp = int(
        block_timestamp.replace(tzinfo=timezone.utc).timestamp()
    )
    return {
        "transactionHash": event["tx_hash"],
        "logIndex": hex(int(event["log_offset"])),
        "blockNumber": hex(int(event["block_height"])),
        "blockTimestamp": hex(unix_timestamp),
        "topics": event["raw_log_topics"],
        "data": event["raw_log_data"],
    }


def decode_rows(events: list[dict[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    ingested_at = datetime.utcnow()
    for event in events:
        topics = event.get("raw_log_topics") or []
        if not topics or topics[0].lower() != TRANSFER_TOPIC:
            continue
        try:
            decoded = decode_transfer_log(covalent_to_rpc_log(event))
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
            print(f"Skipping malformed Covalent event: {exc}", file=sys.stderr)
    return rows


def flush_rows(client: Any, rows: list[list[Any]]) -> int:
    if not rows:
        return 0
    client.insert("raw_transfers", rows, column_names=COLUMN_NAMES)
    count = len(rows)
    rows.clear()
    return count


async def fetch_page(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    page_number: int,
    from_date: str,
    starting_block: int,
    ending_block: int,
) -> tuple[list[dict[str, Any]], bool | None]:
    params = {
        "starting-block": str(starting_block),
        "ending-block": str(ending_block),
        "page-size": str(PAGE_SIZE),
        "page-number": str(page_number),
        "from": from_date,
    }
    async with session.get(
        COVALENT_API_URL, params=params, auth=auth
    ) as response:
        body = await response.text()
        if response.status >= 400:
            raise RuntimeError(f"Covalent HTTP {response.status}: {body[:500]}")
        try:
            payload = await response.json()
        except (aiohttp.ContentTypeError, ValueError) as exc:
            raise RuntimeError("Covalent returned invalid JSON") from exc

    if payload.get("error"):
        raise RuntimeError(
            f"Covalent API error: {payload.get('error_message', payload)}"
        )
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise RuntimeError("Covalent response does not contain a data object")
    items = data.get("items") or []
    if not isinstance(items, list):
        raise RuntimeError("Covalent response items are not a list")
    pagination = data.get("pagination") or {}
    return items, pagination.get("has_more")


async def get_current_block(
    session: aiohttp.ClientSession, rpc_url: str
) -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": [],
    }
    async with session.post(rpc_url, json=payload) as response:
        body = await response.text()
        if response.status >= 400:
            raise RuntimeError(f"QuickNode HTTP {response.status}: {body[:500]}")
        try:
            result = await response.json()
        except (aiohttp.ContentTypeError, ValueError) as exc:
            raise RuntimeError("QuickNode returned invalid JSON") from exc
    if "error" in result:
        raise RuntimeError(f"QuickNode RPC error: {result['error']}")
    return int(result["result"], 16)


def populate_materialized_views_if_empty(client: Any) -> None:
    """Populate targets only if active materialized views inserted no rows."""
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


async def backfill() -> int:
    api_key = get_api_key()
    quicknode_url = get_quicknode_url()
    from_date = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    auth = aiohttp.BasicAuth(api_key, "")
    timeout = aiohttp.ClientTimeout(total=120)
    client = get_clickhouse_client()
    pending_rows: list[list[Any]] = []
    total_events = 0
    total_inserted = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            current_block = await get_current_block(session, quicknode_url)
            start_block = current_block - (7 * 24 * 3600 * 2)

            for chunk_start in range(
                start_block, current_block, BLOCK_CHUNK_SIZE
            ):
                chunk_end = min(
                    chunk_start + BLOCK_CHUNK_SIZE - 1, current_block
                )
                page_number = 0
                while True:
                    events, has_more = await fetch_page(
                        session,
                        auth,
                        page_number,
                        from_date,
                        chunk_start,
                        chunk_end,
                    )
                    if not events:
                        break

                    decoded_rows = decode_rows(events)
                    pending_rows.extend(decoded_rows)
                    total_events += len(decoded_rows)
                    while len(pending_rows) >= INSERT_BATCH_SIZE:
                        chunk = pending_rows[:INSERT_BATCH_SIZE]
                        total_inserted += flush_rows(client, chunk)
                        del pending_rows[:INSERT_BATCH_SIZE]

                    print(
                        f"Blocks {chunk_start}-{chunk_end} | "
                        f"Page {page_number} → {len(decoded_rows):,} events "
                        f"(total: {total_events:,})"
                    )
                    if has_more is False:
                        break
                    page_number += 1

        total_inserted += flush_rows(client, pending_rows)
        populate_materialized_views_if_empty(client)
    finally:
        client.close()

    print(f"Inserted {total_inserted:,} rows into payments.raw_transfers")
    return total_inserted


async def main() -> None:
    try:
        await backfill()
        run_dbt()
    except Exception as exc:
        print(f"Covalent backfill failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print("Covalent backfill completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
