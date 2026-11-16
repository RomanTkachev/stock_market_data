"""Настройки загрузчиков рыночных данных.

Значения можно переопределить переменными окружения — они прокидываются из .env
в контейнеры Airflow (см. docker-compose.yaml).
"""

from __future__ import annotations

import os
from datetime import date

SCHEMA = "stock_market"
PRICES_MOEX_TABLE = "prices_moex"
PRICES_YFINANCE_TABLE = "prices_yfinance"
INSTRUMENTS_TABLE = "instruments"

# Списки тикеров. Когда появится потребность менять их без деплоя —
# вынести в Airflow Variable или в таблицу stock_market.instruments.
MOEX_TICKERS: tuple[str, ...] = ("SBER", "GAZP", "LKOH", "YDEX", "T")
YFINANCE_TICKERS: tuple[str, ...] = ("AAPL", "MSFT", "INTC", "BTC-USD", "ETH-USD", "RUB=X")

# Начало истории. Используется однократно (bootstrap): когда в таблице ещё нет
# данных, грузим всю историю, иначе — только окно текущего запуска по logical_date.
HISTORY_START_DATE = date(2020, 1, 1)

# На сколько дней назад расширять окно плановой загрузки: страховка от того,
# что источник отдаёт последние свечи с задержкой.
DEFAULT_LOOKBACK_DAYS = int(os.getenv("LOADER_LOOKBACK_DAYS", "3"))
