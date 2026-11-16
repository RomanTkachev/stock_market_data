{{ config(materialized='table') }}

-- Сплиты с ценой на дату сплита: справочная витрина для BI
select
    s.ticker,
    s.split_date,
    s.ratio,
    p.close as close_on_split_date,
    i.instrument_type,
    i.sector
from {{ ref('stg_splits') }} as s
left join {{ ref('stg_prices') }} as p
    on p.ticker = s.ticker and p.price_date = s.split_date
left join {{ ref('stg_instruments') }} as i
    on i.ticker = s.ticker
