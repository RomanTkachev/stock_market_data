{{ config(materialized='view') }}

select
    ticker,
    split_date,
    ratio
from {{ source('stock_market_raw', 'splits') }}
where ratio > 0
