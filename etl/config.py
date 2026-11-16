"""Настройки загрузчиков рыночных данных.

Значения можно переопределить переменными окружения — они прокидываются из .env
в контейнеры Airflow (см. docker-compose.yaml).
"""

from __future__ import annotations

import os
from datetime import date

SCHEMA = "stock_market"
PRICES_TABLE = "prices"
DIVIDENDS_TABLE = "dividends"
SPLITS_TABLE = "splits"
INSTRUMENTS_TABLE = "instruments"

# Инвестиционная вселенная: класс актива → тикеры Yahoo Finance.
# Используется и загрузчиком цен, и для заполнения справочника instruments.
UNIVERSE: dict[str, tuple[str, ...]] = {
    "stock": ("AAPL", "MSFT", "INTC", "NVDA", "GOOGL", "AMZN", "TSLA"),
    "etf": ("SPY", "QQQ"),
    "crypto": ("BTC-USD", "ETH-USD", "SOL-USD"),
    "fx": ("RUB=X", "EURUSD=X"),
    "index": ("^GSPC", "^IXIC", "^VIX"),
    "commodity": ("GC=F", "CL=F"),
}

TICKERS: tuple[str, ...] = tuple(ticker for symbols in UNIVERSE.values() for ticker in symbols)

# Начало истории. Используется однократно (bootstrap): когда в таблице ещё нет
# данных, грузим всю историю, иначе — только окно текущего запуска по logical_date.
HISTORY_START_DATE = date(2020, 1, 1)

# На сколько дней назад расширять окно плановой загрузки: страховка от того,
# что источник отдаёт последние свечи с задержкой.
DEFAULT_LOOKBACK_DAYS = int(os.getenv("LOADER_LOOKBACK_DAYS", "3"))

