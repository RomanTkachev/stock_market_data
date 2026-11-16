"""Пайплайны загрузки: окно по logical_date, заполнение пропусков, upsert.

Функции вызываются из DAG-ов и возвращают словарь со статистикой (уходит в XCom).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
from sqlalchemy.engine import Engine

from etl import db, moex_source, yfinance_source
from etl.config import (
    DEFAULT_LOOKBACK_DAYS,
    HISTORY_START_DATE,
    MOEX_TICKERS,
    PRICES_MOEX_TABLE,
    PRICES_YFINANCE_TABLE,
    YFINANCE_TICKERS,
)
from etl.dates import dedupe_prices, fill_missing_dates, resolve_load_window

logger = logging.getLogger(__name__)


def load_yfinance_interval(
    interval_start: datetime,
    interval_end: datetime,
    *,
    tickers: Iterable[str] = YFINANCE_TICKERS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    engine: Engine | None = None,
) -> dict:
    """Загружает цены Yahoo Finance за окно запуска DAG-а."""
    engine = engine or db.get_engine()
    window_start, window_end = _resolve_window(
        engine, PRICES_YFINANCE_TABLE, tickers, interval_start, interval_end, lookback_days
    )
    raw = yfinance_source.fetch_daily_prices(tickers, window_start, window_end)
    return _store(engine, PRICES_YFINANCE_TABLE, raw, window_start, window_end, source="yfinance")


def load_moex_interval(
    interval_start: datetime,
    interval_end: datetime,
    *,
    tickers: Iterable[str] = MOEX_TICKERS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    engine: Engine | None = None,
) -> dict:
    """Загружает дневные свечи MOEX за окно запуска DAG-а."""
    engine = engine or db.get_engine()
    moex_source.configure_token()
    window_start, window_end = _resolve_window(
        engine, PRICES_MOEX_TABLE, tickers, interval_start, interval_end, lookback_days
    )
    # У moexalgo верхняя граница включительная, у окна — эксклюзивная
    end_inclusive = window_end - timedelta(days=1)
    raw = moex_source.fetch_daily_prices(tickers, window_start, end_inclusive)
    return _store(engine, PRICES_MOEX_TABLE, raw, window_start, window_end, source="moex")


def _resolve_window(
    engine: Engine,
    table_name: str,
    tickers: Iterable[str],
    interval_start: datetime,
    interval_end: datetime,
    lookback_days: int,
) -> tuple:
    return resolve_load_window(
        has_history=db.has_history(engine, table_name, tickers),
        interval_start=interval_start,
        interval_end=interval_end,
        lookback_days=lookback_days,
        history_start=HISTORY_START_DATE,
    )


def _store(
    engine: Engine,
    table_name: str,
    raw: pd.DataFrame,
    window_start,
    window_end,
    *,
    source: str,
) -> dict:
    if raw.empty:
        logger.warning("%s: за %s..%s данных нет", source, window_start, window_end)

    prices = dedupe_prices(fill_missing_dates(raw))
    inserted = db.upsert_prices(engine, table_name, prices)

    stats = {
        "source": source,
        "table": table_name,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "fetched_rows": int(len(raw)),
        "rows_to_load": int(len(prices)),
        "inserted_rows": int(inserted),
    }
    logger.info(
        "%s: окно %s..%s, получено %d, к загрузке %d, вставлено %d",
        source,
        window_start,
        window_end,
        stats["fetched_rows"],
        stats["rows_to_load"],
        stats["inserted_rows"],
    )
    return stats
