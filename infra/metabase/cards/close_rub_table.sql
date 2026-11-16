-- Последняя доступная цена и доходность по каждому инструменту, в рублях
with latest as (
    select distinct on (ticker)
        ticker,
        instrument_type,
        sector,
        price_date,
        close,
        close_rub,
        daily_return,
        volume
    from stock_market.fct_prices_daily
    where {{asset_class}} and {{ticker}}
    order by ticker, price_date desc
)
select
    ticker,
    instrument_type,
    sector,
    price_date,
    round(close::numeric, 2) as close,
    round(close_rub::numeric, 2) as close_rub,
    round(daily_return::numeric, 4) as daily_return,
    volume
from latest
order by close_rub desc nulls last
