#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .env ]]; then
    echo "ERROR: .env file not found at $REPO_ROOT/.env" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

if [[ -z "${ALCHEMY_BASE_RPC_URL:-}" ]]; then
    echo "ERROR: ALCHEMY_BASE_RPC_URL is not set in .env" >&2
    exit 1
fi

echo "Fetching the latest Base block from Alchemy..."
RPC_RESPONSE="$(
    curl -sS -X POST "$ALCHEMY_BASE_RPC_URL" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","id":1,"params":[]}'
)"

CURRENT_BLOCK="$(
    printf '%s' "$RPC_RESPONSE" | python -c '
import json
import sys

payload = json.load(sys.stdin)
if "error" in payload:
    raise SystemExit("Alchemy RPC error: %s" % payload["error"])
result = payload.get("result")
if not isinstance(result, str):
    raise SystemExit("Alchemy RPC response is missing a hexadecimal result")
print(int(result, 16))
'
)"

START_BLOCK=$((CURRENT_BLOCK - 5))
END_BLOCK=$((CURRENT_BLOCK - 1))

echo "Ingesting Base blocks $START_BLOCK through $END_BLOCK..."
python -m src.producer \
    --start-block "$START_BLOCK" \
    --end-block "$END_BLOCK"

echo "Consuming Kafka messages for up to 30 seconds..."
set +e
if command -v timeout >/dev/null 2>&1; then
    timeout 30 python -m src.consumer
    CONSUMER_STATUS=$?
elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout 30 python -m src.consumer
    CONSUMER_STATUS=$?
else
    python -c '
import subprocess
import sys

process = subprocess.Popen(sys.argv[2:])
try:
    status = process.wait(timeout=float(sys.argv[1]))
except subprocess.TimeoutExpired:
    process.terminate()
    process.wait()
    status = 124
raise SystemExit(status)
' 30 python -m src.consumer
    CONSUMER_STATUS=$?
fi
set -e

if [[ "$CONSUMER_STATUS" -ne 0 && "$CONSUMER_STATUS" -ne 124 ]]; then
    echo "ERROR: consumer exited with status $CONSUMER_STATUS" >&2
    exit "$CONSUMER_STATUS"
fi

echo "Running dbt models..."
(
    cd dbt
    dbt run
)

echo "Latest-block ingestion completed successfully."
