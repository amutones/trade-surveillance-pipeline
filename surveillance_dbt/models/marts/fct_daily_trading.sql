with orders as (
    select * from {{ ref('stg_orders') }}
),

executions as (
    select * from {{ ref('stg_executions') }}
),

daily_summary as (
    select
        o.trade_date,
        o.firm_id,
        o.symbol,
        o.side_label,
        count(distinct o.cl_ord_id) as order_count,
        sum(o.quantity) as total_shares,
        sum(e.notional_value) as total_notional,
        round(avg(e.fill_price)::numeric, 2) as avg_fill_price,
        count(distinct e.venue) as venues_used
    from orders o
    left join executions e on o.cl_ord_id = e.cl_ord_id
    group by 1, 2, 3, 4
)

select * from daily_summary
order by trade_date desc, total_notional desc