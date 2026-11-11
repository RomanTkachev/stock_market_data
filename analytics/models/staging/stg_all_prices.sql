{{ config(materialized='view') }}


WITH source_yfinance AS (

    SELECT * FROM {{ source('stock_market_raw', 'prices_yfinance') }}

),

source_moex AS (

    SELECT * FROM {{ source('stock_market_raw', 'prices_moex') }}

),

-- Объединяем две таблицы в одну
unioned_sources AS (

    SELECT * FROM source_yfinance
    
    UNION ALL

    SELECT * FROM source_moex
)

-- Финальный SELECT из объединенного источника
SELECT
    price_date,
    ticker,
    price
FROM unioned_sources
