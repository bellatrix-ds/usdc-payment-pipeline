# Data model

| Model | Grain | Purpose |
|---|---|---|
| `raw_usdc_transfer_events` | One RPC log observation | Immutable ingestion payload and metadata |
| `canonical_usdc_transfers` | One chain/transaction/log event | Validated, deduplicated transfer fact |
| `dim_addresses` | One address | Address metadata and classifications |
| `fct_usdc_payments` | One canonical transfer | Payment-oriented transfer representation |
| `mart_usdc_daily` | One chain and day | Daily count, volume, sender, and receiver metrics |

Every fact retains chain ID, block number, block hash, transaction hash, log index, event time, ingestion time, and finality status.
