"""Fetch Base USDC Transfer logs from Alchemy and publish them to Kafka."""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from typing import Any

import aiohttp

try:
    from src.common.abi import TRANSFER_TOPIC, decode_transfer_log
    from src.common.idempotency import make_event_id
except ModuleNotFoundError:  # Support direct execution: python src/producer.py
    from common.abi import TRANSFER_TOPIC, decode_transfer_log
    from common.idempotency import make_event_id


def _load_config() -> Any:
    try:
        from src.common import config
    except ModuleNotFoundError:
        from common import config
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    args = parser.parse_args()
    if args.start_block > args.end_block:
        parser.error("--start-block must be less than or equal to --end-block")
    return args


async def fetch_logs_batch(
    session: aiohttp.ClientSession, start: int, end: int
) -> list[dict]:
    config = _load_config()
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [
            {
                "address": config.USDC_CONTRACT_ADDRESS,
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "topics": [TRANSFER_TOPIC],
            }
        ],
    }
    async with session.post(config.ALCHEMY_BASE_RPC_URL, json=request) as response:
        response.raise_for_status()
        payload = await response.json()

    if "error" in payload:
        raise RuntimeError(f"Alchemy RPC error: {payload['error']}")
    result = payload.get("result")
    if not isinstance(result, list):
        raise RuntimeError("Alchemy RPC response did not contain a log result list")
    return result


async def fetch_all_logs(start_block: int, end_block: int) -> list[dict]:
    config = _load_config()
    ranges = [
        (start, min(start + config.BATCH_SIZE - 1, end_block))
        for start in range(start_block, end_block + 1, config.BATCH_SIZE)
    ]
    print(f"Fetching {len(ranges)} Alchemy batch(es)")
    semaphore = asyncio.Semaphore(5)

    async def fetch_with_retry(start: int, end: int) -> tuple[int, int, list[dict]]:
        async with semaphore:
            for attempt in range(4):
                try:
                    logs = await fetch_logs_batch(session, start, end)
                    return start, end, logs
                except Exception:
                    if attempt == 3:
                        raise
                    delay = 2**attempt
                    print(
                        f"Batch {start}-{end} failed; retrying in {delay}s "
                        f"({attempt + 1}/3)"
                    )
                    await asyncio.sleep(delay)
        raise RuntimeError("unreachable")

    timeout = aiohttp.ClientTimeout(total=60)
    all_logs: list[dict] = []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_with_retry(start, end) for start, end in ranges]
        for completed in asyncio.as_completed(tasks):
            start, end, logs = await completed
            all_logs.extend(logs)
            print(f"Completed batch {start}-{end}: {len(logs)} log(s)")
    return all_logs


def publish_to_kafka(logs: list[dict]) -> int:
    from confluent_kafka import Producer

    config = _load_config()
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    published = 0
    delivery_errors: list[str] = []

    def on_delivery(error: Any, _message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for log in logs:
        try:
            decoded = decode_transfer_log(log)
            if decoded is None:
                print("Skipping non-Transfer event", file=sys.stderr)
                continue
            message = {
                **decoded,
                "event_id": make_event_id(
                    decoded["tx_hash"], decoded["log_index"]
                ),
                "ingested_at": datetime.utcnow().isoformat(),
            }
            producer.produce(
                topic=config.KAFKA_TOPIC,
                key=decoded["from_address"],
                value=json.dumps(message, default=str),
                callback=on_delivery,
            )
            producer.poll(0)
            published += 1
        except (KeyError, TypeError, ValueError) as exc:
            print(f"Skipping log that failed decoding: {exc}", file=sys.stderr)

    undelivered = producer.flush()
    if undelivered:
        raise RuntimeError(f"Kafka could not deliver {undelivered} message(s)")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
    return published


async def main() -> None:
    args = parse_args()
    try:
        _load_config()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        logs = await fetch_all_logs(args.start_block, args.end_block)
        published = publish_to_kafka(logs)
    except Exception as exc:
        print(f"Pipeline error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Published {published} USDC Transfer event(s) to Kafka")


if __name__ == "__main__":
    asyncio.run(main())
