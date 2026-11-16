{{ config(materialized='table') }}

-- Справочник инструментов: атрибуты для срезания фактов
select
    ticker,
    name,
    instrument_type,
    currency,
    exchange,
    sector,
    industry,
    market_cap
from {{ ref('stg_instruments') }}
