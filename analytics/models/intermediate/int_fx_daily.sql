{{ config(materialized='view') }}

-- Дневные курсы валют из того же источника цен: USD/RUB и EUR/USD
select
    price_date,
    max(case when ticker = 'RUB=X' then close end) as usd_rub,
    max(case when ticker = 'EURUSD=X' then close end) as eur_usd
from {{ ref('stg_prices') }}
where ticker in ('RUB=X', 'EURUSD=X')
group by price_date
