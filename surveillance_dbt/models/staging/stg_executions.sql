with source as (
    select * from {{ source('raw', 'executions') }}
),

transformed as (
    select
        exec_id,
        cl_ord_id,
        symbol,
        side,
        case when side = '1' then 'BUY' else 'SELL' end as side_label,
        fill_qty,
        fill_price,
        fill_qty * fill_price as notional_value,
        transact_time,
        date(transact_time) as trade_date,
        venue,
        created_at
    from source
)

select * from transformed