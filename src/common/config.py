"""Environment-backed configuration for the USDC payment pipeline."""

import os

from dotenv import load_dotenv


load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


ALCHEMY_BASE_RPC_URL: str = _required("ALCHEMY_BASE_RPC_URL")
QUICKNODE_BASE_RPC_URL: str = os.getenv("QUICKNODE_BASE_RPC_URL", "")
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE: str = os.getenv("CLICKHOUSE_DATABASE", "payments")
CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")
USDC_CONTRACT_ADDRESS: str = os.getenv(
    "USDC_CONTRACT_ADDRESS",
    "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
)
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "usdc.transfers")
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "2000"))
