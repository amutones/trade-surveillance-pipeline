with daily_activity as (
    select
        trade_date,
        firm_id,
        symbol,
        sum(case when side_label = 'BUY' then total_shares else 0 end) as buy_shares,
        sum(case when side_label = 'SELL' then total_shares else 0 end) as sell_shares,
        sum(total_notional) as total_notional
    from {{ ref('fct_daily_trading') }}
    group by 1, 2, 3
),

flagged as (
    select
        trade_date,
        firm_id,
        symbol,
        buy_shares,
        sell_shares,
        total_notional,
        case 
            when buy_shares > 0 and sell_shares > 0 then true
            else false
        end as wash_trade_flag
    from daily_activity
)

select * from flagged
where wash_trade_flag = true
order by trade_date desc, total_notional desc