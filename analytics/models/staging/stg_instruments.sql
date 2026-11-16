{{ config(materialized='view') }}

select
    ticker,
    name,
    instrument_type,
    lower(currency) as currency,
    exchange,
    sector,
    industry,
    market_cap
from {{ source('stock_market_raw', 'instruments') }}
