-- Динамика цен, нормированная к 100 на начало выбранного периода
with base as (
    select
        price_date,
        ticker,
        instrument_type,
        close_rub,
        first_value(close_rub) over (partition by ticker order by price_date) as first_close
    from stock_market.fct_prices_daily
    where {{period}} and {{asset_class}} and {{ticker}}
      and close_rub is not null
      and close_rub <> 0
)
select
    price_date,
    ticker,
    instrument_type,
    round(close_rub / first_close * 100, 2) as index_100
from base
order by price_date, ticker
