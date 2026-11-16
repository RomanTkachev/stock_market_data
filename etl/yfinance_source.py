"""Загрузка дневных котировок из Yahoo Finance."""

from __future__ import annotations

import logging
from datetime import date
from typing import Iterable

import pandas as pd
import yfinance as yf

from etl.dates import empty_prices_frame

logger = logging.getLogger(__name__)


def fetch_daily_prices(tickers: Iterable[str], start: date, end: date) -> pd.DataFrame:
    """Цены закрытия за ``[start, end)`` в длинном формате [price_date, ticker, price]."""
    ticker_list = tuple(tickers)
    if not ticker_list or end <= start:
        return empty_prices_frame()

    raw = yf.download(
        tickers=list(ticker_list),
        start=start,
        end=end,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    if raw.empty:
        logger.warning("yfinance не вернул данных за %s..%s", start, end)
        return empty_prices_frame()

    return to_long_prices(raw, ticker_list)


def to_long_prices(raw: pd.DataFrame, tickers: Iterable[str] = ()) -> pd.DataFrame:
    """Преобразует wide-формат yfinance (Date × (ticker, field)) в длинный."""
    ticker_list = tuple(tickers)

    if isinstance(raw.columns, pd.MultiIndex):
        frame = (
            raw.stack(level=0, future_stack=True)
            .rename_axis(["price_date", "ticker"])
            .reset_index()
        )
    else:
        # Один тикер: yfinance отдаёт плоские колонки (Close, High, ...)
        if len(ticker_list) != 1:
            raise ValueError("не удалось определить тикеры в данных yfinance")
        frame = raw.rename_axis("price_date").reset_index()
        frame["ticker"] = ticker_list[0]

    long = frame[["price_date", "ticker", "Close"]].rename(columns={"Close": "price"})
    long = long.assign(
        price_date=pd.to_datetime(long["price_date"]),
        ticker=long["ticker"].astype(str),
    )
    return long.dropna(subset=["price"]).reset_index(drop=True)
