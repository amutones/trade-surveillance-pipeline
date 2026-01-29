with orders as (
    select * from {{ ref('stg_orders') }}
),

executions as (
    select * from {{ ref('stg_executions') }}
)

select
    date(o.transact_time) as trade_date,
    o.firm_id,
    e.venue,
    count(*) as execution_count,
    sum(e.fill_qty) as total_shares,
    round(sum(e.notional_value)::numeric, 2) as total_notional,
    round(avg(e.fill_price)::numeric, 2) as avg_fill_price
from orders o
join executions e on o.cl_ord_id = e.cl_ord_id
group by date(o.transact_time), o.firm_id, e.venue