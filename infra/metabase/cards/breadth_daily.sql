-- Ширина рынка: сколько инструментов выросло и сколько упало за день
select
    price_date,
    sum(advancers) as advancers,
    sum(decliners) as decliners
from stock_market.fct_market_overview
where {{period}}
group by price_date
order by price_date
