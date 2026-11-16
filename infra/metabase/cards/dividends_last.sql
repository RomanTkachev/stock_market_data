-- Последние дивиденды и доходность на дату отсечки
select
    ticker,
    ex_date,
    amount,
    round(close_on_ex_date::numeric, 2) as close_on_ex_date,
    round(dividend_yield::numeric, 5) as dividend_yield,
    instrument_type
from stock_market.fct_dividends
where {{period}} and {{asset_class}}
order by ex_date desc
limit 100
