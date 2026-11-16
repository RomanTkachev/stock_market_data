{{ config(materialized='view') }}

-- Цены, обогащённые атрибутами инструмента и пересчитанные в рубли
select
    p.price_date,
    p.ticker,
    i.instrument_type,
    i.sector,
    i.industry,
    i.currency,
    p.open,
    p.high,
    p.low,
    p.close,
    p.adj_close,
    p.volume,
    fx.usd_rub,
    fx.eur_usd,
    case
        when i.currency = 'rub' then p.close
        when i.currency = 'eur' then p.close * fx.usd_rub / nullif(fx.eur_usd, 0)
        else p.close * fx.usd_rub
    end as close_rub
from {{ ref('stg_prices') }} as p
left join {{ ref('stg_instruments') }} as i
    on i.ticker = p.ticker
left join {{ ref('int_fx_daily') }} as fx
    on fx.price_date = p.price_date
