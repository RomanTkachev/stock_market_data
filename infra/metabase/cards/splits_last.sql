-- Сплиты акций с ценой на дату сплита
select
    ticker,
    split_date,
    ratio,
    round(close_on_split_date::numeric, 2) as close_on_split_date,
    instrument_type
from stock_market.fct_splits
where {{period}} and {{asset_class}} and {{ticker}}
order by split_date desc
