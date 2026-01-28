with executions as (
    select * from {{ ref('stg_executions') }}
)

select
    date(transact_time) as trade_date,
    venue,
    count(*) as execution_count,
    sum(fill_qty) as total_shares,
    round(sum(notional_value)::numeric, 2) as total_notional
from executions
group by date(transact_time), venue