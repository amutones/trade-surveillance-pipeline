with orders as (
    select * from {{ ref('stg_orders') }}
),

executions as (
    select * from {{ ref('stg_executions') }}
)

select
    date(o.transact_time) as trade_date,
    o.firm_id,
    o.symbol,
    count(*) as order_count,
    sum(o.quantity) as total_shares,
    sum(case when o.side = 'BUY' then o.quantity else 0 end) as buy_shares,
    sum(case when o.side = 'SELL' then o.quantity else 0 end) as sell_shares,
    round(sum(e.notional_value)::numeric, 2) as total_notional
from orders o
join executions e on o.cl_ord_id = e.cl_ord_id
group by date(o.transact_time), o.firm_id, o.symbol