with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    cl_ord_id,
    symbol,
    case when side = '1' then 'BUY' else 'SELL' end as side,
    case when order_type = '1' then 'MARKET' else 'LIMIT' end as order_type,
    quantity,
    transact_time,
    account_id,
    firm_id,
    case 
        when ord_status = '0' then 'NEW'
        when ord_status = '2' then 'FILLED'
        when ord_status = '4' then 'CANCELED'
        else 'UNKNOWN'
    end as order_status
from source