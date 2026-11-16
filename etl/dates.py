"""Календарная логика: окно загрузки, заполнение пропусков, дедупликация."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

PRICE_COLUMNS = ("price_date", "ticker", "open", "high", "low", "close", "adj_close", "volume")
PRICE_VALUE_COLUMNS = ("open", "high", "low", "close", "adj_close", "volume")


def empty_prices_frame() -> pd.DataFrame:
    """Пустой DataFrame с колонками таблиц цен."""
    return pd.DataFrame(columns=list(PRICE_COLUMNS))


def build_load_window(
    interval_start: datetime,
    interval_end: datetime,
    lookback_days: int = 0,
) -> tuple[date, date]:
    """Окно загрузки ``[start, end)`` для планового запуска.

    ``interval_end`` — эксклюзивная верхняя граница: данные за текущий день
    ещё не сформированы (yfinance вообще не отдаёт свечу текущего дня).
    ``lookback_days`` расширяет окно назад, чтобы догрузить дни, которые
    источник вернул с задержкой.
    """
    if lookback_days < 0:
        raise ValueError("lookback_days не может быть отрицательным")
    if interval_end <= interval_start:
        raise ValueError("interval_end должен быть больше interval_start")
    return interval_start.date() - timedelta(days=lookback_days), interval_end.date()


def resolve_load_window(
    has_history: bool,
    interval_start: datetime,
    interval_end: datetime,
    lookback_days: int = 0,
    history_start: date | None = None,
) -> tuple[date, date]:
    """Окно загрузки: вся история для пустой таблицы, иначе окно запуска.

    Ветка ``has_history=False`` нужна для первичного наполнения: без неё
    catchup с ``start_date`` DAG-а никогда не загрузит историю до этой даты.
    """
    if not has_history:
        if history_start is None:
            raise ValueError("для пустой таблицы нужно передать history_start")
        return history_start, interval_end.date()
    return build_load_window(interval_start, interval_end, lookback_days)


def fill_missing_dates(
    df: pd.DataFrame,
    date_col: str = "price_date",
    ticker_col: str = "ticker",
    value_cols: tuple[str, ...] = PRICE_VALUE_COLUMNS,
) -> pd.DataFrame:
    """Заполняет пропущенные календарные дни для каждого тикера.

    Внутри диапазона min..max по тикеру пропуски закрываются forward/back fill,
    чтобы в БД был непрерывный календарь (нужно для конверсии валют и джойнов по дате).
    Объём торгов на неторговых днях обнуляется: переносить его нельзя.
    Пустые календарные дни за границами диапазона не добавляются.
    """
    if df.empty:
        return empty_prices_frame()

    frame = df.copy()
    frame[date_col] = pd.to_datetime(frame[date_col])
    columns = [column for column in value_cols if column in frame.columns]

    filled_frames = []
    for ticker_name, group in frame.groupby(ticker_col):
        series = group.set_index(date_col)[columns].sort_index()
        min_date, max_date = series.index.min(), series.index.max()
        if pd.isna(min_date) or pd.isna(max_date):
            continue

        full_range = pd.date_range(start=min_date, end=max_date, freq="D", name=date_col)
        filled = series.reindex(full_range).ffill().bfill()
        if "volume" in filled.columns:
            is_trading_day = filled.index.isin(series.index)
            filled["volume"] = filled["volume"].where(is_trading_day, 0)
        filled[ticker_col] = ticker_name
        filled_frames.append(filled)

    if not filled_frames:
        return empty_prices_frame()

    result = pd.concat(filled_frames).reset_index()
    return result[[date_col, ticker_col, *columns]].reset_index(drop=True)


def dedupe_prices(
    df: pd.DataFrame,
    date_col: str = "price_date",
    ticker_col: str = "ticker",
) -> pd.DataFrame:
    """Убирает дубли по (ticker, price_date) внутри одного батча.

    Нужно перед upsert: PostgreSQL не принимает два конфликтующих значения
    в одном INSERT.
    """
    if df.empty:
        return empty_prices_frame()
    return (
        df.drop_duplicates(subset=[ticker_col, date_col], keep="last")
        .sort_values([ticker_col, date_col])
        .reset_index(drop=True)
    )
