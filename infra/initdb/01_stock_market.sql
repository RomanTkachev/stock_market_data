-- Bootstrap-скрипт БД с рыночными данными (сервис market-postgres в docker-compose).
--
-- Скрипт выполняется автоматически официальным образом postgres ТОЛЬКО при первом
-- создании data-каталога (пустой том market-db-volume). Для уже существующего тома
-- применить вручную:
--   docker compose exec -T market-postgres psql -U market -d market -f - < infra/initdb/01_stock_market.sql
--
-- Загрузчики Airflow (dags/scripts/*_loader.py) и dbt (analytics/models) ожидают
-- именно эту схему и набор колонок.

CREATE SCHEMA IF NOT EXISTS stock_market;

-- Справочник инструментов: используется dbt-моделями stg_instruments и fct_all_prices
CREATE TABLE IF NOT EXISTS stock_market.instruments (
    instrument_id   SERIAL       PRIMARY KEY,
    ticker          VARCHAR(20)  NOT NULL UNIQUE,
    name            TEXT,
    instrument_type VARCHAR(50),
    currency        VARCHAR(10),
    is_russian      BOOLEAN
);

-- Дневные котировки MOEX (moexalgo)
CREATE TABLE IF NOT EXISTS stock_market.prices_moex (
    price_date DATE         NOT NULL,
    ticker     VARCHAR(20)  NOT NULL,
    price      NUMERIC(18, 6)
);

-- Дневные котировки Yahoo Finance (акции, криптовалюты, валюты)
CREATE TABLE IF NOT EXISTS stock_market.prices_yfinance (
    price_date DATE         NOT NULL,
    ticker     VARCHAR(20)  NOT NULL,
    price      NUMERIC(18, 6)
);

-- Индексы под запросы загрузчиков + защита от дублей.
-- UNIQUE обязателен для идемпотентной загрузки: INSERT ... ON CONFLICT (ticker, price_date).
CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_moex_ticker_date
    ON stock_market.prices_moex (ticker, price_date);

CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_yfinance_ticker_date
    ON stock_market.prices_yfinance (ticker, price_date);

-- Начальное наполнение справочника под текущие списки тикеров в DAG-ах.
-- currency намеренно в нижнем регистре: dbt-модель fct_all_prices сравнивает с 'usd'/'rub'.
INSERT INTO stock_market.instruments (ticker, name, instrument_type, currency, is_russian) VALUES
    ('SBER',    'Сбербанк',       'stock',    'rub', TRUE),
    ('GAZP',    'Газпром',        'stock',    'rub', TRUE),
    ('LKOH',    'Лукойл',         'stock',    'rub', TRUE),
    ('YDEX',    'Яндекс',         'stock',    'rub', TRUE),
    ('T',       'Т-Технологии',   'stock',    'rub', TRUE),
    ('AAPL',    'Apple',          'stock',    'usd', FALSE),
    ('MSFT',    'Microsoft',      'stock',    'usd', FALSE),
    ('INTC',    'Intel',          'stock',    'usd', FALSE),
    ('BTC-USD', 'Bitcoin',        'crypto',   'usd', FALSE),
    ('ETH-USD', 'Ethereum',       'crypto',   'usd', FALSE),
    ('RUB=X',   'USD/RUB',        'currency', 'rub', TRUE)
ON CONFLICT (ticker) DO NOTHING;
