"""Загрузка рыночных данных и корпоративных действий из Yahoo Finance."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Iterable

import pandas as pd
import yfinance as yf

from etl.dates import PRICE_VALUE_COLUMNS, empty_prices_frame

logger = logging.getLogger(__name__)

# Колонки yfinance → колонки БД
PRICE_FIELD_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def fetch_daily_prices(tickers: Iterable[str], start: date, end: date) -> pd.DataFrame:
    """Дневные OHLCV-свечи за ``[start, end)`` в длинном формате."""
    ticker_list = tuple(tickers)
    if not ticker_list or end <= start:
        return empty_prices_frame()

    raw = yf.download(
        tickers=list(ticker_list),
        start=start,
        end=end,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,  # нужны и Close (для отчётности), и Adj Close (для доходностей)
        actions=False,
        progress=False,
    )
    if raw.empty:
        logger.warning("yfinance не вернул данные за %s..%s", start, end)
        return empty_prices_frame()

    return to_long_prices(raw, ticker_list)


def to_long_prices(raw: pd.DataFrame, tickers: Iterable[str] = ()) -> pd.DataFrame:
    """Преобразует wide-формат yfinance (Date × (ticker, field)) в длинный OHLCV."""
    ticker_list = tuple(tickers)

    if isinstance(raw.columns, pd.MultiIndex):
        frame = (
            raw.stack(level=0, future_stack=True)
            .rename_axis(["price_date", "ticker"])
            .reset_index()
        )
    else:
        # Один тикер: yfinance отдаёт плоские колонки (Open, High, ...)
        if len(ticker_list) != 1:
            raise ValueError("не удалось определить тикеры в данных yfinance")
        frame = raw.rename_axis("price_date").reset_index()
        frame["ticker"] = ticker_list[0]

    frame = frame.rename(columns=PRICE_FIELD_MAP)
    for column in PRICE_VALUE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    long = frame[["price_date", "ticker", *PRICE_VALUE_COLUMNS]].copy()
    long["price_date"] = pd.to_datetime(long["price_date"])
    long["ticker"] = long["ticker"].astype(str)
    return long.dropna(subset=["close"]).reset_index(drop=True)


def fetch_actions(tickers: Iterable[str], start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Дивиденды и сплиты за ``[start, end)``.

    Возвращает пару DataFrame: dividends (ticker, ex_date, amount) и
    splits (ticker, split_date, ratio).
    """
    ticker_list = tuple(tickers)
    dividends, splits, failed = [], [], []

    for symbol in ticker_list:
        try:
            actions = yf.Ticker(symbol).actions
        except Exception as exc:  # noqa: BLE001 - сбой по одному тикеру не валит весь запуск
            logger.warning("Не удалось получить корпоративные действия по %s: %s", symbol, exc)
            failed.append(symbol)
            continue

        if actions is None or actions.empty:
            continue

        index = pd.DatetimeIndex(actions.index)
        if index.tz is not None:
            index = index.tz_localize(None)
        frame = actions.assign(action_date=index.normalize())
        frame = frame[(frame["action_date"].dt.date >= start) & (frame["action_date"].dt.date < end)]
        if frame.empty:
            continue

        paid = frame[frame["Dividends"].fillna(0) > 0]
        if not paid.empty:
            dividends.append(
                pd.DataFrame(
                    {
                        "ticker": symbol,
                        "ex_date": paid["action_date"].dt.date,
                        "amount": paid["Dividends"].astype(float),
                    }
                )
            )

        split = frame[frame["Stock Splits"].fillna(0) > 0]
        if not split.empty:
            splits.append(
                pd.DataFrame(
                    {
                        "ticker": symbol,
                        "split_date": split["action_date"].dt.date,
                        "ratio": split["Stock Splits"].astype(float),
                    }
                )
            )

    if ticker_list and len(failed) == len(ticker_list):
        raise RuntimeError(
            "yfinance не вернул корпоративные действия ни по одному тикеру: " + ", ".join(failed)
        )

    dividends_frame = (
        pd.concat(dividends, ignore_index=True)
        if dividends
        else pd.DataFrame(columns=["ticker", "ex_date", "amount"])
    )
    splits_frame = (
        pd.concat(splits, ignore_index=True)
        if splits
        else pd.DataFrame(columns=["ticker", "split_date", "ratio"])
    )
    return dividends_frame, splits_frame


def fetch_instruments_meta(universe: dict[str, tuple[str, ...]]) -> list[dict]:
    """Метаданные справочника: класс актива, валюта, биржа, отрасль, капитализация."""
    rows = []
    updated_at = datetime.now(timezone.utc)

    for asset_class, symbols in universe.items():
        for symbol in symbols:
            try:
                info = yf.Ticker(symbol).info or {}
            except Exception as exc:  # noqa: BLE001 - .info бывает нестабильным (429/401)
                logger.warning("Не удалось получить метаданные по %s: %s", symbol, exc)
                info = {}

            rows.append(
                {
                    "ticker": symbol,
                    "name": info.get("shortName") or info.get("longName"),
                    "instrument_type": asset_class,
                    "currency": (info.get("currency") or "").lower() or None,
                    "exchange": info.get("exchange"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "market_cap": info.get("marketCap"),
                    "updated_at": updated_at,
                }
            )

    return rows

