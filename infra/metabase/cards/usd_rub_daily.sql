-- Курс USD/RUB
select
    price_date,
    round(usd_rub::numeric, 4) as usd_rub
from stock_market.int_fx_daily
where {{period}} and usd_rub is not null
order by price_date
