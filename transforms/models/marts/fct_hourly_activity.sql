with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    date(transact_time) as trade_date,
    extract(hour from transact_time)::int as hour,
    count(*) as order_count,
    sum(quantity) as total_shares
from orders
group by date(transact_time), extract(hour from transact_time)
order by hour