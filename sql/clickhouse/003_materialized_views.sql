CREATE TABLE IF NOT EXISTS payments.mv_hourly_activity
(
    hour DateTime,
    transfer_count UInt64,
    volume_raw UInt256,
    unique_senders UInt64
)
ENGINE = MergeTree
ORDER BY (hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS payments.mv_hourly_activity_mv
TO payments.mv_hourly_activity
AS
SELECT
    toStartOfHour(block_timestamp) AS hour,
    count() AS transfer_count,
    sum(amount_raw) AS volume_raw,
    uniq(from_address) AS unique_senders
FROM payments.raw_transfers
GROUP BY hour;

CREATE TABLE IF NOT EXISTS payments.mv_daily_summary
(
    day Date,
    transfer_count UInt64,
    volume_raw UInt256,
    unique_senders UInt64,
    avg_amount_raw Float64
)
ENGINE = MergeTree
ORDER BY (day);

CREATE MATERIALIZED VIEW IF NOT EXISTS payments.mv_daily_summary_mv
TO payments.mv_daily_summary
AS
SELECT
    toDate(block_timestamp) AS day,
    count() AS transfer_count,
    sum(amount_raw) AS volume_raw,
    uniq(from_address) AS unique_senders,
    avg(toFloat64(amount_raw)) AS avg_amount_raw
FROM payments.raw_transfers
GROUP BY day;
