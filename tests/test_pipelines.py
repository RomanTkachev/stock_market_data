from datetime import date, datetime

import pandas as pd

from etl import pipelines
from etl.dates import empty_prices_frame

INTERVAL_START = datetime(2026, 11, 9, 8, 0)
INTERVAL_END = datetime(2026, 11, 10, 8, 0)


class FakeEngine:
    """Заглушка движка: пайплайны в тестах не ходят в БД."""

    def connect(self):  # pragma: no cover - не используется, т.к. db.* подменены
        raise AssertionError("БД в тестах не используется")

    def begin(self):  # pragma: no cover
        raise AssertionError("БД в тестах не используется")


def _prices_frame(rows: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "price_date": pd.to_datetime(["2026-11-09"] * rows),
            "ticker": ["AAPL"] * rows,
            "open": [1.0] * rows,
            "high": [2.0] * rows,
            "low": [0.5] * rows,
            "close": [1.5] * rows,
            "adj_close": [1.4] * rows,
            "volume": [100] * rows,
        }
    )


def _stub_prices_fetch(monkeypatch, windows):
    def fake_fetch(tickers, start, end):
        windows.append((start, end))
        return _prices_frame()

    monkeypatch.setattr(pipelines.yfinance_source, "fetch_daily_prices", fake_fetch)


def _stub_upsert(monkeypatch, stored_rows: list[int], function_name: str = "upsert_prices"):
    def fake_upsert(engine, df, **kwargs):
        stored_rows.append(len(df))
        return len(df)

    monkeypatch.setattr(pipelines.db, function_name, fake_upsert)


def test_load_prices_interval_bootstraps_history_for_empty_table(monkeypatch):
    windows, stored = [], []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: False)
    _stub_prices_fetch(monkeypatch, windows)
    _stub_upsert(monkeypatch, stored)

    stats = pipelines.load_prices_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), engine=FakeEngine()
    )

    assert windows == [(pipelines.HISTORY_START_DATE, date(2026, 11, 10))]
    assert stats["written_rows"] == 1
    assert stats["fetched_rows"] == 1
    assert stats["source"] == "prices"


def test_load_prices_interval_uses_lookback_when_history_exists(monkeypatch):
    windows, stored = [], []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: True)
    _stub_prices_fetch(monkeypatch, windows)
    _stub_upsert(monkeypatch, stored)

    pipelines.load_prices_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), lookback_days=3, engine=FakeEngine()
    )

    assert windows == [(date(2026, 11, 6), date(2026, 11, 10))]


def test_load_prices_interval_reports_empty_window(monkeypatch):
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        pipelines.yfinance_source,
        "fetch_daily_prices",
        lambda tickers, start, end: empty_prices_frame(),
    )
    _stub_upsert(monkeypatch, [])

    stats = pipelines.load_prices_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), engine=FakeEngine()
    )

    assert stats["fetched_rows"] == 0
    assert stats["written_rows"] == 0


def test_load_actions_interval_bootstraps_history(monkeypatch):
    windows = []
    monkeypatch.setattr(pipelines.db, "count_rows", lambda *args, **kwargs: 0)

    def fake_fetch(tickers, start, end):
        windows.append((start, end))
        dividends = pd.DataFrame(
            [{"ticker": "AAPL", "ex_date": date(2026, 11, 5), "amount": 1.5}]
        )
        splits = pd.DataFrame(columns=["ticker", "split_date", "ratio"])
        return dividends, splits

    monkeypatch.setattr(pipelines.yfinance_source, "fetch_actions", fake_fetch)
    monkeypatch.setattr(pipelines.db, "upsert_dividends", lambda engine, df, **kw: len(df))
    monkeypatch.setattr(pipelines.db, "upsert_splits", lambda engine, df, **kw: len(df))

    stats = pipelines.load_actions_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), engine=FakeEngine()
    )

    assert windows == [(pipelines.HISTORY_START_DATE, date(2026, 11, 10))]
    assert stats["dividends_written"] == 1
    assert stats["splits_written"] == 0
    assert stats["source"] == "actions"


def test_load_actions_interval_uses_window_when_actions_exist(monkeypatch):
    windows = []
    monkeypatch.setattr(pipelines.db, "count_rows", lambda *args, **kwargs: 5)
    monkeypatch.setattr(
        pipelines.yfinance_source,
        "fetch_actions",
        lambda tickers, start, end: (
            windows.append((start, end)),
            (pd.DataFrame(columns=["ticker", "ex_date", "amount"]),
             pd.DataFrame(columns=["ticker", "split_date", "ratio"])),
        )[1],
    )
    monkeypatch.setattr(pipelines.db, "upsert_dividends", lambda engine, df, **kw: 0)
    monkeypatch.setattr(pipelines.db, "upsert_splits", lambda engine, df, **kw: 0)

    pipelines.load_actions_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), lookback_days=3, engine=FakeEngine()
    )

    assert windows == [(date(2026, 11, 6), date(2026, 11, 10))]


def test_load_instruments_upserts_metadata(monkeypatch):
    monkeypatch.setattr(
        pipelines.yfinance_source,
        "fetch_instruments_meta",
        lambda universe: [{"ticker": "AAPL", "instrument_type": "stock"}],
    )
    stored = []
    monkeypatch.setattr(pipelines.db, "upsert_instruments", lambda engine, rows, **kw: stored.append(rows) or len(rows))

    stats = pipelines.load_instruments(universe={"stock": ("AAPL",)}, engine=FakeEngine())

    assert stats == {"source": "instruments", "tickers": 1, "written": 1}
    assert stored[0][0]["ticker"] == "AAPL"
