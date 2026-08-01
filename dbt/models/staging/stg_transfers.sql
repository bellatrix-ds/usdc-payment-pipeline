select
    event_id,
    tx_hash,
    cast(log_index as Int32) as log_index,
    cast(block_number as Int64) as block_number,
    block_timestamp,
    lower(from_address) as from_address,
    lower(to_address) as to_address,
    toDecimal128(toString(amount_raw), 0) as amount_raw,
    ingested_at
from {{ source('payments', 'raw_transfers') }}
