{{ config(materialized='view') }}

select
    price_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    coalesce(volume, 0) as volume
from {{ source('stock_market_raw', 'prices') }}
where close is not null
