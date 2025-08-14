/*
The first query gets a running total based on event type.
The second query was used to create the unioned table of all events on aave ethereum for usdt, which was used in the first query.

*/

WITH p AS (
  SELECT
    TO_DATE('2020-01-08') AS start_date,
    TO_DATE('2025-08-13') AS end_date
),
all_dates as (
    SELECT DATEADD('day', SEQ4(), start_date) AS date
    FROM p
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 40000))   -- big constant
    WHERE SEQ4() <= DATEDIFF('day', start_date, end_date)  -- inclusive
    ORDER BY date
),
all_actions as (
    select 
        -- block_number, log_index, event_index,
        block_timestamp, 
        amount, 
        case 
            when lending_event in ('deposits', 'repayments') then 1
            when lending_event in ('withdrawals', 'loans') then -1
            when lending_event in ('liquidations') and event_index = 1 then 1 -- repay tokens
            when lending_event in ('liquidations') and event_index = 2 then -1 -- collateral tokens 
            else 0 -- there should be no 0s in this table
        end effect_on_vault,
        amount*effect_on_vault delta_on_vault,
        SUM(COALESCE(delta_on_vault, 0)) OVER (
            partition by chain, token_address
            ORDER BY block_number, log_index, event_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vault_running_total, 
        -- lag(amount, 1, 0) over(partition by chain, token_address order by block_number, log_index, event_index) prev_vault_bal,
        -- lending_event,
        row_number() over(partition by chain, token_address, date_trunc(day, block_timestamp) order by block_number desc, log_index desc, event_index desc) one_is_eod
    from KATE_DYNAMIC_TABLES.PUBLIC.AAVE_ETH_USDT_VAULT
    qualify row_number() over(partition by chain, token_address, date_trunc(day, block_timestamp) order by block_number desc, log_index desc, event_index desc) = 1
)
select 
    a.date,
    b.vault_running_total,
    c.supply_usd,
    vault_running_total/supply_usd 
from all_dates a 
left join all_actions b 
    on date_trunc(day, block_timestamp) = date 
left join (
    select date supply_date, supply_usd 
    from ALLIUM_CROSSCHAIN.STABLECOIN.SUPPLY_BETA
    where chain = 'ethereum'
        and token_address = '0xdac17f958d2ee523a2206206994597c13d831ec7' -- usdt 
) c 
    on a.date = c.supply_date
-- where vault_running_total is null -- check no nulls 
order by date desc 
;

CREATE OR REPLACE DYNAMIC TABLE kate_dynamic_tables.public.aave_eth_usdt_vault
    TARGET_LAG = '4 hours'
    WAREHOUSE = compute_adhoc
    REFRESH_MODE = auto
    INITIALIZE = on_create
    AS

with filters as (
    select 
    'ethereum' as chain,
    'aave' as project, 
    '0xdac17f958d2ee523a2206206994597c13d831ec7' as token -- usdt 
)
select 
    chain,
    token_address,
    token_symbol,
    block_number,
    block_timestamp,
    log_index,
    1 event_index,
    amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.DEPOSITS
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and token_address in (select token from filters)

union all 

select 
    chain,
    token_address,
    token_symbol,
    block_number,
    block_timestamp,
    log_index,
    1 event_index,
    amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.WITHDRAWALS 
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and token_address in (select token from filters)

union all 

select 
    chain,
    token_address,
    token_symbol,
    block_number,
    block_timestamp,
    log_index,
    1 event_index,
    amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.LOANS  
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and token_address in (select token from filters)

union all 

select 
    chain,
    token_address,
    token_symbol,
    block_number,
    block_timestamp,
    log_index,
    1 event_index,
    amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.REPAYMENTS  
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and token_address in (select token from filters)

union all 

select 
    chain,
    repay_token_address,
    repay_token_symbol,
    block_number,
    block_timestamp,
    log_index,
    1 event_index,
    repay_amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.LIQUIDATIONS 
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and repay_token_address in (select token from filters)

union all 

select 
    chain,
    token_address,
    token_symbol,
    block_number,
    block_timestamp,
    log_index,
    2 event_index,
    amount,
    lending_event
from ALLIUM_CROSSCHAIN.LENDING.LIQUIDATIONS 
where 
    chain in (select chain from filters)
    and project in (select project from filters)
    and token_address in (select token from filters)
;
