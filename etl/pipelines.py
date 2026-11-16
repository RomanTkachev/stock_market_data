"""Пайплайны загрузки: окно по logical_date, заполнение пропусков, upsert.

Функции вызываются из DAG-ов и возвращают словарь со статистикой (уходит в XCom).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from sqlalchemy.engine import Engine

from etl import db, yfinance_source
from etl.config import DEFAULT_LOOKBACK_DAYS, HISTORY_START_DATE, TICKERS, UNIVERSE
from etl.dates import dedupe_prices, fill_missing_dates, resolve_load_window

logger = logging.getLogger(__name__)


def load_prices_interval(
    interval_start: datetime,
    interval_end: datetime,
    *,
    tickers: Iterable[str] = TICKERS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    engine: Engine | None = None,
) -> dict:
    """Загружает OHLCV-цены за окно запуска DAG-а."""
    engine = engine or db.get_engine()
    window_start, window_end = resolve_load_window(
        has_history=db.has_history(engine, tickers),
        interval_start=interval_start,
        interval_end=interval_end,
        lookback_days=lookback_days,
        history_start=HISTORY_START_DATE,
    )

    raw = yfinance_source.fetch_daily_prices(tickers, window_start, window_end)
    prices = dedupe_prices(fill_missing_dates(raw))
    written = db.upsert_prices(engine, prices)

    return _stats(
        source="prices",
        window_start=window_start,
        window_end=window_end,
        fetched=len(raw),
        prepared=len(prices),
        written=written,
    )


def load_actions_interval(
    interval_start: datetime,
    interval_end: datetime,
    *,
    tickers: Iterable[str] = TICKERS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    engine: Engine | None = None,
) -> dict:
    """Загружает дивиденды и сплиты за окно запуска DAG-а."""
    engine = engine or db.get_engine()
    has_actions = bool(db.count_rows(engine, db.DIVIDENDS) + db.count_rows(engine, db.SPLITS))
    window_start, window_end = resolve_load_window(
        has_history=has_actions,
        interval_start=interval_start,
        interval_end=interval_end,
        lookback_days=lookback_days,
        history_start=HISTORY_START_DATE,
    )

    dividends, splits = yfinance_source.fetch_actions(tickers, window_start, window_end)
    dividends_written = db.upsert_dividends(engine, dividends)
    splits_written = db.upsert_splits(engine, splits)

    stats = _stats(
        source="actions",
        window_start=window_start,
        window_end=window_end,
        fetched=len(dividends) + len(splits),
        prepared=len(dividends) + len(splits),
        written=dividends_written + splits_written,
    )
    stats["dividends_written"] = dividends_written
    stats["splits_written"] = splits_written
    return stats


def load_instruments(
    *,
    universe: dict[str, tuple[str, ...]] = UNIVERSE,
    engine: Engine | None = None,
) -> dict:
    """Обновляет справочник инструментов (upsert по тикеру)."""
    engine = engine or db.get_engine()
    rows = yfinance_source.fetch_instruments_meta(universe)
    written = db.upsert_instruments(engine, rows)

    logger.info("instruments: получено %d, записано %d", len(rows), written)
    return {"source": "instruments", "tickers": len(rows), "written": written}


def _stats(
    *,
    source: str,
    window_start,
    window_end,
    fetched: int,
    prepared: int,
    written: int,
) -> dict:
    stats = {
        "source": source,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "fetched_rows": int(fetched),
        "rows_to_load": int(prepared),
        "written_rows": int(written),
    }
    logger.info(
        "%s: окно %s..%s, получено %d, к загрузке %d, записано %d",
        source,
        window_start,
        window_end,
        fetched,
        prepared,
        written,
    )
    return stats
