select
    toDate(block_timestamp) as day,
    count() as transfer_count,
    sum(amount_usdc) as volume_usdc,
    uniq(from_address) as unique_senders,
    avg(toFloat64(amount_usdc)) as avg_amount_usdc
from {{ ref('int_transfers_enriched') }}
group by day
order by day
