with source as (
    select * from {{ source('raw', 'orders') }}
),

transformed as (
    select
        cl_ord_id,
        symbol,
        side,
        case when side = '1' then 'BUY' else 'SELL' end as side_label,
        order_type,
        quantity,
        transact_time,
        date(transact_time) as trade_date,
        account_id,
        firm_id,
        ord_status,
        created_at
    from source
)

select * from transformed