with source as (
    select * from {{ source('raw', 'executions') }}
)

select
    exec_id,
    cl_ord_id,
    symbol,
    case when side = '1' then 'BUY' else 'SELL' end as side,
    fill_qty,
    fill_price,
    round((fill_qty * fill_price)::numeric, 2) as notional_value,
    transact_time,
    venue
from source