-- Волатильность доходностей за 30 дней
select
    price_date,
    ticker,
    round(volatility_30d::numeric, 5) as volatility_30d
from stock_market.fct_prices_daily
where {{period}} and {{asset_class}} and {{ticker}}
  and volatility_30d is not null
order by price_date, ticker
