from datetime import date, datetime

import pandas as pd
import pytest

from etl.dates import build_load_window, dedupe_prices, fill_missing_dates, resolve_load_window

INTERVAL_START = datetime(2026, 11, 9, 8, 0)
INTERVAL_END = datetime(2026, 11, 10, 8, 0)


def test_build_load_window_returns_exclusive_end():
    assert build_load_window(INTERVAL_START, INTERVAL_END) == (date(2026, 11, 9), date(2026, 11, 10))


def test_build_load_window_applies_lookback():
    start, end = build_load_window(INTERVAL_START, INTERVAL_END, lookback_days=3)
    assert (start, end) == (date(2026, 11, 6), date(2026, 11, 10))


def test_build_load_window_rejects_negative_lookback():
    with pytest.raises(ValueError):
        build_load_window(INTERVAL_START, INTERVAL_END, lookback_days=-1)


def test_build_load_window_rejects_empty_interval():
    with pytest.raises(ValueError):
        build_load_window(INTERVAL_END, INTERVAL_START)


def test_resolve_load_window_bootstraps_full_history():
    window = resolve_load_window(
        has_history=False,
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
        lookback_days=3,
        history_start=date(2020, 1, 1),
    )
    assert window == (date(2020, 1, 1), date(2026, 11, 10))


def test_resolve_load_window_requires_history_start_for_empty_table():
    with pytest.raises(ValueError):
        resolve_load_window(
            has_history=False,
            interval_start=INTERVAL_START,
            interval_end=INTERVAL_END,
            history_start=None,
        )


def test_resolve_load_window_uses_interval_when_history_exists():
    window = resolve_load_window(
        has_history=True,
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
        lookback_days=3,
        history_start=date(2020, 1, 1),
    )
    assert window == (date(2026, 11, 6), date(2026, 11, 10))


def test_fill_missing_dates_fills_weekend_with_previous_price():
    df = pd.DataFrame(
        {
            "price_date": [date(2026, 11, 6), date(2026, 11, 9)],
            "ticker": ["AAPL", "AAPL"],
            "price": [100.0, 110.0],
        }
    )

    filled = fill_missing_dates(df)

    assert list(filled.columns) == ["price_date", "ticker", "price"]
    assert len(filled) == 4  # пт, сб, вс, пн
    assert list(filled["price"]) == [100.0, 100.0, 100.0, 110.0]
    assert set(filled["ticker"]) == {"AAPL"}


def test_fill_missing_dates_processes_tickers_independently():
    df = pd.DataFrame(
        {
            "price_date": [
                date(2026, 11, 2),
                date(2026, 11, 3),
                date(2026, 11, 2),
                date(2026, 11, 6),
            ],
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "price": [1.0, 2.0, 10.0, 20.0],
        }
    )

    filled = fill_missing_dates(df)

    assert filled.groupby("ticker").size().to_dict() == {"AAPL": 2, "MSFT": 5}


def test_fill_missing_dates_returns_empty_frame_for_empty_input():
    filled = fill_missing_dates(pd.DataFrame())
    assert filled.empty
    assert list(filled.columns) == ["price_date", "ticker", "price"]


def test_dedupe_prices_keeps_last_row_per_ticker_and_date():
    df = pd.DataFrame(
        {
            "price_date": [pd.Timestamp("2026-11-09"), pd.Timestamp("2026-11-09")],
            "ticker": ["AAPL", "AAPL"],
            "price": [1.0, 2.0],
        }
    )

    deduped = dedupe_prices(df)

    assert len(deduped) == 1
    assert deduped.iloc[0]["price"] == 2.0
