-- Топ-5 инструментов по объёму торгов за день
select
    price_date,
    ticker,
    instrument_type,
    volume,
    round(turnover::numeric, 0) as turnover,
    volume_rank
from stock_market.fct_volume_leaders
where volume_rank <= 5
  and {{period}} and {{asset_class}} and {{ticker}}
order by price_date desc, volume_rank
limit 100
