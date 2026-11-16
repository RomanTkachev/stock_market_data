"""Загрузка дневных котировок MOEX (Algopack / moexalgo)."""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Iterable

import pandas as pd
from moexalgo import Ticker, session

from etl.dates import empty_prices_frame

logger = logging.getLogger(__name__)


def configure_token(token: str | None = None) -> None:
    """Прокидывает токен Algopack в сессию moexalgo."""
    resolved = token or os.getenv("MOEX_API_TOKEN")
    if not resolved:
        logger.warning("MOEX_API_TOKEN не задан, запросы к MOEX могут быть ограничены")
    session.TOKEN = resolved


def fetch_daily_prices(tickers: Iterable[str], start: date, end: date) -> pd.DataFrame:
    """Дневные свечи за ``[start, end]`` (обе границы включительно)."""
    ticker_list = tuple(tickers)
    if not ticker_list or end < start:
        return empty_prices_frame()

    frames = []
    for symbol in ticker_list:
        try:
            candles = Ticker(symbol).candles(start=start, end=end, period="1d")
        except Exception as exc:  # noqa: BLE001 - ошибка по одному тикеру не должна валить весь запуск
            logger.error("Не удалось загрузить данные MOEX по %s: %s", symbol, exc)
            continue

        if candles is None or candles.empty:
            logger.warning("MOEX не вернул данных по %s за %s..%s", symbol, start, end)
            continue

        frame = candles.reset_index().rename(columns={"begin": "price_date", "close": "price"})
        if "price_date" not in frame.columns or "price" not in frame.columns:
            logger.error("Неожиданный формат свечей MOEX по %s: %s", symbol, list(frame.columns))
            continue

        frame = frame.assign(price_date=pd.to_datetime(frame["price_date"]), ticker=symbol)
        frames.append(frame[["price_date", "ticker", "price"]].dropna(subset=["price"]))

    if not frames:
        return empty_prices_frame()
    return pd.concat(frames, ignore_index=True)
