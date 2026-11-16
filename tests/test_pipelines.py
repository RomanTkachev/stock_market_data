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


def _stub_fetch(monkeypatch, module, window_calls, rows: int = 1):
    def fake_fetch(tickers, start, end):
        window_calls.append((start, end))
        return pd.DataFrame(
            {
                "price_date": pd.to_datetime(["2026-11-09"] * rows),
                "ticker": ["AAPL"] * rows,
                "price": [1.0] * rows,
            }
        )

    monkeypatch.setattr(module, "fetch_daily_prices", fake_fetch)


def _stub_upsert(monkeypatch, stored):
    def fake_upsert(engine, table_name, df, **kwargs):
        stored.append((table_name, len(df)))
        return len(df)

    monkeypatch.setattr(pipelines.db, "upsert_prices", fake_upsert)


def test_load_yfinance_interval_bootstraps_history_for_empty_table(monkeypatch):
    windows, stored = [], []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: False)
    _stub_fetch(monkeypatch, pipelines.yfinance_source, windows)
    _stub_upsert(monkeypatch, stored)

    stats = pipelines.load_yfinance_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), engine=FakeEngine()
    )

    assert windows == [(pipelines.HISTORY_START_DATE, date(2026, 11, 10))]
    assert stats["inserted_rows"] == 1
    assert stats["fetched_rows"] == 1
    assert stats["table"] == "prices_yfinance"


def test_load_yfinance_interval_uses_lookback_when_history_exists(monkeypatch):
    windows, stored = [], []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: True)
    _stub_fetch(monkeypatch, pipelines.yfinance_source, windows)
    _stub_upsert(monkeypatch, stored)

    pipelines.load_yfinance_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), lookback_days=3, engine=FakeEngine()
    )

    assert windows == [(date(2026, 11, 6), date(2026, 11, 10))]


def test_load_moex_interval_uses_inclusive_end(monkeypatch):
    windows, stored = [], []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: True)
    monkeypatch.setattr(pipelines.moex_source, "configure_token", lambda *args, **kwargs: None)
    _stub_fetch(monkeypatch, pipelines.moex_source, windows)
    _stub_upsert(monkeypatch, stored)

    stats = pipelines.load_moex_interval(
        INTERVAL_START, INTERVAL_END, tickers=("SBER",), lookback_days=3, engine=FakeEngine()
    )

    # окно [6 Nov, 10 Nov), но moexalgo ждёт включительную верхнюю границу
    assert windows == [(date(2026, 11, 6), date(2026, 11, 9))]
    assert stats["table"] == "prices_moex"
    assert stats["source"] == "moex"


def test_load_interval_reports_empty_window(monkeypatch):
    stored = []
    monkeypatch.setattr(pipelines.db, "has_history", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        pipelines.yfinance_source,
        "fetch_daily_prices",
        lambda tickers, start, end: empty_prices_frame(),
    )
    _stub_upsert(monkeypatch, stored)

    stats = pipelines.load_yfinance_interval(
        INTERVAL_START, INTERVAL_END, tickers=("AAPL",), engine=FakeEngine()
    )

    assert stats["fetched_rows"] == 0
    assert stats["inserted_rows"] == 0
