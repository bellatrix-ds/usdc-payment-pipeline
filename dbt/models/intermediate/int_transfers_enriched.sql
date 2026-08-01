with transfers as (
    select
        *,
        toDecimal128(amount_raw, 6) / 1000000 as amount_usdc
    from {{ ref('stg_transfers') }}
)

select
    *,
    multiIf(
        amount_usdc >= 100000, 'mega',
        amount_usdc >= 10000, 'whale',
        amount_usdc >= 1000, 'large',
        amount_usdc >= 100, 'medium',
        amount_usdc >= 10, 'small',
        'micro'
    ) as size_bucket
from transfers
