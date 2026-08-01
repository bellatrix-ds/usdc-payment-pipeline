CREATE TABLE IF NOT EXISTS payments.raw_transfers
(
    event_id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime,
    from_address String,
    to_address String,
    amount_raw UInt256,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (from_address, block_timestamp, event_id)
SETTINGS index_granularity = 8192;
