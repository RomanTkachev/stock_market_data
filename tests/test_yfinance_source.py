from datetime import date

import pandas as pd
import pytest

from etl import yfinance_source


def _wide_frame() -> pd.DataFrame:
    index = pd.to_datetime(["2026-11-09", "2026-11-10"])
    fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    columns = pd.MultiIndex.from_product([["AAPL", "MSFT"], fields])
    rows = [
        [1.0, 2.0, 0.5, 1.5, 1.4, 100, 3.0, 4.0, 2.5, 3.5, 3.4, 300],
        [2.0, 3.0, 1.5, 2.5, 2.4, 200, 4.0, 5.0, 3.5, 4.5, 4.4, 400],
    ]
    return pd.DataFrame(rows, index=index, columns=columns)


class FakeTicker:
    def __init__(self, actions: pd.DataFrame | None = None, info: dict | None = None, error: Exception | None = None):
        self._actions = actions if actions is not None else pd.DataFrame()
        self._info = info or {}
        self._error = error

    @property
    def actions(self) -> pd.DataFrame:
        if self._error is not None:
            raise self._error
        return self._actions

    @property
    def info(self) -> dict:
        if self._error is not None:
            raise self._error
        return self._info


def _patch_tickers(monkeypatch, by_symbol: dict[str, FakeTicker]):
    monkeypatch.setattr(yfinance_source.yf, "Ticker", lambda symbol: by_symbol[symbol])


def test_to_long_prices_converts_multiindex_frame_to_ohlcv():
    long = yfinance_source.to_long_prices(_wide_frame(), ["AAPL", "MSFT"])

    assert list(long.columns) == ["price_date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    assert len(long) == 4
    assert set(long["ticker"]) == {"AAPL", "MSFT"}

    aapl = long[(long["ticker"] == "AAPL") & (long["price_date"] == pd.Timestamp("2026-11-10"))].iloc[0]
    assert aapl["close"] == 2.5
    assert aapl["adj_close"] == 2.4
    assert aapl["volume"] == 200


def test_to_long_prices_drops_rows_without_close():
    frame = _wide_frame()
    frame.loc[:, ("AAPL", "Close")] = [1.5, float("nan")]

    long = yfinance_source.to_long_prices(frame, ["AAPL", "MSFT"])

    assert len(long) == 3


def test_to_long_prices_supports_single_ticker_flat_frame():
    index = pd.to_datetime(["2026-11-09"])
    raw = pd.DataFrame(
        {"Open": [9.0], "High": [11.0], "Low": [8.0], "Close": [10.0], "Adj Close": [9.9], "Volume": [50]},
        index=index,
    )

    long = yfinance_source.to_long_prices(raw, ["AAPL"])

    assert long.to_dict("records") == [
        {
            "price_date": pd.Timestamp("2026-11-09"),
            "ticker": "AAPL",
            "open": 9.0,
            "high": 11.0,
            "low": 8.0,
            "close": 10.0,
            "adj_close": 9.9,
            "volume": 50,
        }
    ]


def test_to_long_prices_requires_tickers_for_flat_frame():
    with pytest.raises(ValueError):
        yfinance_source.to_long_prices(pd.DataFrame({"Close": [1.0]}), ())


def test_fetch_actions_splits_dividends_and_splits_by_window(monkeypatch):
    index = pd.DatetimeIndex(pd.to_datetime(["2026-11-02", "2026-11-05", "2026-11-10"]))
    actions = pd.DataFrame({"Dividends": [0.0, 1.5, 0.0], "Stock Splits": [0.0, 0.0, 4.0]}, index=index)
    _patch_tickers(monkeypatch, {"AAPL": FakeTicker(actions=actions)})

    dividends, splits = yfinance_source.fetch_actions(("AAPL",), date(2026, 11, 1), date(2026, 11, 11))

    assert dividends.to_dict("records") == [{"ticker": "AAPL", "ex_date": date(2026, 11, 5), "amount": 1.5}]
    assert splits.to_dict("records") == [{"ticker": "AAPL", "split_date": date(2026, 11, 10), "ratio": 4.0}]


def test_fetch_actions_respects_window(monkeypatch):
    index = pd.DatetimeIndex(pd.to_datetime(["2026-11-05"]))
    actions = pd.DataFrame({"Dividends": [1.5], "Stock Splits": [0.0]}, index=index)
    _patch_tickers(monkeypatch, {"AAPL": FakeTicker(actions=actions)})

    dividends, splits = yfinance_source.fetch_actions(("AAPL",), date(2026, 11, 10), date(2026, 11, 11))

    assert dividends.empty
    assert splits.empty


def test_fetch_actions_raises_when_all_tickers_failed(monkeypatch):
    _patch_tickers(
        monkeypatch,
        {"AAPL": FakeTicker(error=RuntimeError("boom")), "MSFT": FakeTicker(error=RuntimeError("boom"))},
    )

    with pytest.raises(RuntimeError, match="ни по одному тикеру"):
        yfinance_source.fetch_actions(("AAPL", "MSFT"), date(2026, 11, 1), date(2026, 11, 11))


def test_fetch_instruments_meta_maps_info_fields(monkeypatch):
    info = {
        "shortName": "Apple Inc.",
        "currency": "USD",
        "exchange": "NMS",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "marketCap": 3_000_000_000_000,
    }
    _patch_tickers(monkeypatch, {"AAPL": FakeTicker(info=info)})

    rows = yfinance_source.fetch_instruments_meta({"stock": ("AAPL",)})

    assert len(rows) == 1
    row = rows[0]
    assert row["ticker"] == "AAPL"
    assert row["name"] == "Apple Inc."
    assert row["instrument_type"] == "stock"
    assert row["currency"] == "usd"
    assert row["exchange"] == "NMS"
    assert row["sector"] == "Technology"
    assert row["market_cap"] == 3_000_000_000_000
    assert row["updated_at"] is not None


def test_fetch_instruments_meta_keeps_ticker_when_info_fails(monkeypatch):
    _patch_tickers(monkeypatch, {"AAPL": FakeTicker(error=RuntimeError("429"))})

    rows = yfinance_source.fetch_instruments_meta({"stock": ("AAPL",)})

    assert len(rows) == 1
    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["name"] is None
    assert rows[0]["instrument_type"] == "stock"
