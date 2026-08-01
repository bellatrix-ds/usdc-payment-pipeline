select *
from {{ ref('int_transfers_enriched') }}
where amount_raw != 0
