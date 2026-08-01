select
    toStartOfHour(block_timestamp) as hour,
    count() as transfer_count,
    sum(amount_usdc) as volume_usdc,
    uniq(from_address) as unique_senders
from {{ ref('int_transfers_enriched') }}
group by hour
order by hour
