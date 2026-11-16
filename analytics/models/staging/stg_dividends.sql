{{ config(materialized='view') }}

select
    ticker,
    ex_date,
    amount
from {{ source('stock_market_raw', 'dividends') }}
where amount > 0
