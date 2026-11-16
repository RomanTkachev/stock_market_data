-- Миграция для уже существующих БД: удаляет дубли и создаёт уникальные индексы,
-- без которых невозможна идемпотентная загрузка (INSERT ... ON CONFLICT).
--
-- Применить:
--   docker compose exec -T market-postgres psql -U market -d market -f - < infra/migrations/001_unique_prices.sql
--
-- Скрипт идемпотентен: его можно запускать повторно.

BEGIN;

-- Оставляем по одной строке на (ticker, price_date); при дублях выживает последняя запись (ctid больше)
DELETE FROM stock_market.prices_moex a
USING stock_market.prices_moex b
WHERE a.ctid < b.ctid
  AND a.ticker = b.ticker
  AND a.price_date = b.price_date;

DELETE FROM stock_market.prices_yfinance a
USING stock_market.prices_yfinance b
WHERE a.ctid < b.ctid
  AND a.ticker = b.ticker
  AND a.price_date = b.price_date;

-- Старые неуникальные индексы заменяются уникальными
DROP INDEX IF EXISTS stock_market.ix_prices_moex_ticker_date;
DROP INDEX IF EXISTS stock_market.ix_prices_yfinance_ticker_date;

CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_moex_ticker_date
    ON stock_market.prices_moex (ticker, price_date);

CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_yfinance_ticker_date
    ON stock_market.prices_yfinance (ticker, price_date);

COMMIT;
