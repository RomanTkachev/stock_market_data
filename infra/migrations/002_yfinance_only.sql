-- Миграция 002: проект переведён на один источник (Yahoo Finance) и полный OHLCV.
--
-- Применить:
--   docker compose exec -T market-postgres psql -U market -d market -f - < infra/migrations/002_yfinance_only.sql
--
-- Скрипт идемпотентен: его можно запускать повторно.

BEGIN;

-- 1. prices_yfinance → prices, price → close
DO $$
BEGIN
    IF to_regclass('stock_market.prices_yfinance') IS NOT NULL
       AND to_regclass('stock_market.prices') IS NULL THEN
        ALTER TABLE stock_market.prices_yfinance RENAME TO prices;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stock_market'
          AND table_name = 'prices'
          AND column_name = 'price'
    ) THEN
        ALTER TABLE stock_market.prices RENAME COLUMN price TO close;
    END IF;
END $$;

-- 2. Колонки OHLCV
ALTER TABLE stock_market.prices
    ADD COLUMN IF NOT EXISTS open      NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS high      NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS low       NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS adj_close NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS volume    BIGINT;

-- 3. Уникальный индекс под новым именем таблицы
ALTER INDEX IF EXISTS stock_market.ux_prices_yfinance_ticker_date RENAME TO ux_prices_ticker_date;
ALTER INDEX IF EXISTS stock_market.ix_prices_yfinance_ticker_date RENAME TO ux_prices_ticker_date;
CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_ticker_date
    ON stock_market.prices (ticker, price_date);

-- 4. Старые dbt-модели зависят от колонок, которые меняются ниже; dbt пересоздаст их в новом виде
DROP VIEW IF EXISTS stock_market.stg_instruments CASCADE;
DROP VIEW IF EXISTS stock_market.stg_all_prices CASCADE;
DROP TABLE IF EXISTS stock_market.fct_all_prices CASCADE;

-- 4.1 Справочник инструментов: было только под MOEX
ALTER TABLE stock_market.instruments DROP COLUMN IF EXISTS is_russian;
ALTER TABLE stock_market.instruments
    ADD COLUMN IF NOT EXISTS exchange   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS sector     VARCHAR(60),
    ADD COLUMN IF NOT EXISTS industry   VARCHAR(80),
    ADD COLUMN IF NOT EXISTS market_cap NUMERIC(24, 2),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

DELETE FROM stock_market.instruments WHERE ticker IN ('SBER', 'GAZP', 'LKOH', 'YDEX', 'T');

INSERT INTO stock_market.instruments (ticker, instrument_type, currency) VALUES
    ('AAPL',    'stock',     'usd'),
    ('MSFT',    'stock',     'usd'),
    ('INTC',    'stock',     'usd'),
    ('NVDA',    'stock',     'usd'),
    ('GOOGL',   'stock',     'usd'),
    ('AMZN',    'stock',     'usd'),
    ('TSLA',    'stock',     'usd'),
    ('SPY',     'etf',       'usd'),
    ('QQQ',     'etf',       'usd'),
    ('BTC-USD', 'crypto',    'usd'),
    ('ETH-USD', 'crypto',    'usd'),
    ('SOL-USD', 'crypto',    'usd'),
    ('RUB=X',   'fx',        'rub'),
    ('EURUSD=X','fx',        'usd'),
    ('^GSPC',   'index',     'usd'),
    ('^IXIC',   'index',     'usd'),
    ('^VIX',    'index',     'usd'),
    ('GC=F',    'commodity', 'usd'),
    ('CL=F',    'commodity', 'usd')
ON CONFLICT (ticker) DO UPDATE
    SET instrument_type = EXCLUDED.instrument_type,
        currency = COALESCE(stock_market.instruments.currency, EXCLUDED.currency);

-- 5. Корпоративные действия
CREATE TABLE IF NOT EXISTS stock_market.dividends (
    ticker  VARCHAR(20)    NOT NULL,
    ex_date DATE           NOT NULL,
    amount  NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (ticker, ex_date)
);

CREATE TABLE IF NOT EXISTS stock_market.splits (
    ticker     VARCHAR(20)    NOT NULL,
    split_date DATE           NOT NULL,
    ratio      NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (ticker, split_date)
);

-- 6. MOEX больше не используется (таблица была пустой; при необходимости сделайте pg_dump заранее)
DROP TABLE IF EXISTS stock_market.prices_moex;

-- 7. Опционально: старые строки цен содержат только close, без OHLCV/Adj Close.
--    Чтобы догрузить историю в новом формате, очистите таблицу — DAG выполнит bootstrap:
-- TRUNCATE stock_market.prices;

COMMIT;
