-- Выбранная акция (фильтр «Тикер») против индекса S&P 500, нормировано к 100
with series as (
    select
        price_date,
        ticker,
        close_rub
    from stock_market.fct_prices_daily
    where ({{ticker}} or ticker = '^GSPC')
      and {{period}}
      and close_rub is not null
      and close_rub <> 0
),
base as (
    select
        price_date,
        ticker,
        close_rub,
        first_value(close_rub) over (partition by ticker order by price_date) as first_close
    from series
)
select
    price_date,
    ticker,
    round(close_rub / first_close * 100, 2) as index_100
from base
order by price_date, ticker
