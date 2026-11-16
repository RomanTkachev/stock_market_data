{{ config(materialized='table') }}

-- Лидеры по объёму торгов за день
select
    price_date,
    ticker,
    instrument_type,
    volume,
    close,
    close_rub,
    volume * close as turnover,
    row_number() over (partition by price_date order by volume desc) as volume_rank
from {{ ref('fct_prices_daily') }}
where volume > 0
