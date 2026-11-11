{{ config(materialized='view') }}

SELECT
    instrument_id,
    ticker,
    name,
    instrument_type,
    currency,
    is_russian
FROM {{ source('stock_market_raw', 'instruments') }}