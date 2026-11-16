-- Bootstrap-скрипт БД с рыночными данными (сервис market-postgres в docker-compose).
--
-- Скрипт выполняется автоматически официальным образом postgres ТОЛЬКО при первом
-- создании data-каталога (пустой том market-db-volume). Для уже существующего тома
-- применить миграции из infra/migrations.
--
-- Загрузчики Airflow (etl/*) и dbt (analytics/models) ожидают именно эту схему.

CREATE SCHEMA IF NOT EXISTS stock_market;

-- Справочник инструментов: заполняется загрузчиком из метаданных Yahoo Finance
CREATE TABLE IF NOT EXISTS stock_market.instruments (
    instrument_id   SERIAL       PRIMARY KEY,
    ticker          VARCHAR(20)  NOT NULL UNIQUE,
    name            VARCHAR(200),
    instrument_type VARCHAR(20),  -- stock | etf | crypto | fx | index | commodity
    currency        VARCHAR(10),
    exchange        VARCHAR(20),
    sector          VARCHAR(60),
    industry        VARCHAR(80),
    market_cap      NUMERIC(24, 2),
    updated_at      TIMESTAMPTZ  DEFAULT now()
);

-- Дневные свечи (Yahoo Finance): Close — как в отчётах, Adj Close — с поправкой на дивиденды и сплиты
CREATE TABLE IF NOT EXISTS stock_market.prices (
    price_date DATE         NOT NULL,
    ticker     VARCHAR(20)  NOT NULL,
    open       NUMERIC(18, 6),
    high       NUMERIC(18, 6),
    low        NUMERIC(18, 6),
    close      NUMERIC(18, 6) NOT NULL,
    adj_close  NUMERIC(18, 6),
    volume     BIGINT
);

-- UNIQUE обязателен для идемпотентной загрузки: INSERT ... ON CONFLICT (ticker, price_date)
CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_ticker_date
    ON stock_market.prices (ticker, price_date);

-- Дивиденды и сплиты (корпоративные действия)
CREATE TABLE IF NOT EXISTS stock_market.dividends (
    ticker  VARCHAR(20)   NOT NULL,
    ex_date DATE          NOT NULL,
    amount  NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (ticker, ex_date)
);

CREATE TABLE IF NOT EXISTS stock_market.splits (
    ticker     VARCHAR(20)   NOT NULL,
    split_date DATE          NOT NULL,
    ratio      NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (ticker, split_date)
);

-- Начальное наполнение справочника: класс актива и валюта известны заранее,
-- остальные атрибуты (название, отрасль, капитализация) догружает reference-DAG.
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
ON CONFLICT (ticker) DO NOTHING;
