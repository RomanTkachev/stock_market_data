-- Средняя дневная доходность по классам активов
select
    price_date,
    instrument_type,
    avg_daily_return
from stock_market.fct_market_overview
where {{period}} and {{asset_class}}
order by price_date, instrument_type
