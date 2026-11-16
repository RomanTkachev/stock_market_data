{{ config(materialized='table') }}

-- Дневной факт по инструментам: OHLCV, цены в рублях и производные метрики
with prices as (

    select * from {{ ref('int_prices_enriched') }}

),

returns as (

    select
        *,
        adj_close / lag(adj_close) over (partition by ticker order by price_date) - 1 as daily_return,
        lag(close) over (partition by ticker order by price_date) as prev_close
    from prices

),

with_metrics as (

    select
        price_date,
        ticker,
        instrument_type,
        sector,
        industry,
        currency,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        usd_rub,
        eur_usd,
        close_rub,
        daily_return,
        -- 20.04.2020 нефть CL=F закрылась по -37.63: логарифм берём только для положительных цен
        case when 1 + daily_return > 0 then ln(1 + daily_return) end as log_return,
        (open - prev_close) / nullif(prev_close, 0) as gap_pct,
        (high - low) / nullif(close, 0) as range_pct,
        avg(adj_close) over w7 as ma_7,
        avg(adj_close) over w30 as ma_30,
        stddev_samp(daily_return) over w30 as volatility_30d
    from returns
    window
        w7 as (partition by ticker order by price_date rows between 6 preceding and current row),
        w30 as (partition by ticker order by price_date rows between 29 preceding and current row)

)

select * from with_metrics

