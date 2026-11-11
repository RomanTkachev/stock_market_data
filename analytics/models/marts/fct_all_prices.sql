{{ config(materialized='table') }}


-- Стейджингвая модель с ценами
WITH all_prices AS (

    SELECT * FROM {{ ref('stg_all_prices') }}

),

-- Стейджингвая модель с инструментами
instruments AS (

    SELECT * FROM {{ ref('stg_instruments') }}

),

-- Рассчитываем подневный курс рубля
rub_exchange_rate AS (

    SELECT
        price_date,
        price AS rate_to_usd
    FROM all_prices
    WHERE ticker = 'RUB=X'

)

-- Соединяем все
SELECT
    pr.price_date,
    pr.ticker,
    inst.name AS ticker_name,
    inst.instrument_type,
    pr.price AS original_price,
    inst.currency,
    
    -- приводим все к рублям
    CASE
        WHEN inst.currency = 'usd' THEN pr.price * re.rate_to_usd
        WHEN inst.currency = 'rub' THEN pr.price 
        ELSE NULL -- если незнакомая валюта
    END AS price_rub
    
FROM all_prices AS pr

-- Присоединяем информацию об инструменте
LEFT JOIN instruments AS inst ON pr.ticker = inst.ticker

-- Присоединяем курс рубля
LEFT JOIN rub_exchange_rate AS re ON pr.price_date = re.price_date
