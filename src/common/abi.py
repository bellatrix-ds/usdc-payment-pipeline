"""Manual decoding helpers for ERC-20 Transfer event logs."""

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)


def decode_transfer_log(log: dict) -> dict:
    """Decode an ``eth_getLogs`` USDC Transfer result without web3.py."""
    topics = log["topics"]
    if len(topics) < 3:
        raise ValueError("Transfer log must contain topic0, from, and to topics")

    return {
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "block_number": int(log["blockNumber"], 16),
        "block_timestamp": int(log.get("blockTimestamp", "0x0"), 16),
        "from_address": f"0x{topics[1][-40:]}".lower(),
        "to_address": f"0x{topics[2][-40:]}".lower(),
        "amount_raw": int(log["data"], 16),
    }
