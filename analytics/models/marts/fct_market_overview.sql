{{ config(materialized='table') }}

-- Рыночная картина дня по классам активов: средняя доходность, ширина рынка, объёмы
select
    price_date,
    instrument_type,
    count(*) filter (where daily_return is not null) as tickers_with_return,
    avg(daily_return) as avg_daily_return,
    count(*) filter (where daily_return > 0) as advancers,
    count(*) filter (where daily_return < 0) as decliners,
    count(*) filter (where daily_return > 0)::numeric
        / nullif(count(*) filter (where daily_return <> 0), 0) as breadth,
    sum(volume) as total_volume
from {{ ref('fct_prices_daily') }}
group by price_date, instrument_type
