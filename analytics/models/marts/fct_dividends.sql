{{ config(materialized='table') }}

-- Дивиденды с доходностью на дату отсечки
select
    d.ticker,
    d.ex_date,
    d.amount,
    p.close as close_on_ex_date,
    d.amount / nullif(p.close, 0) as dividend_yield,
    i.instrument_type,
    i.sector
from {{ ref('stg_dividends') }} as d
left join {{ ref('stg_prices') }} as p
    on p.ticker = d.ticker and p.price_date = d.ex_date
left join {{ ref('stg_instruments') }} as i
    on i.ticker = d.ticker
