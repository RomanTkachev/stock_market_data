from datetime import date

import pandas as pd
import pytest

from etl import moex_source


class FakeTicker:
    def __init__(self, symbol, frame=None, error=None):
        self.symbol = symbol
        self._frame = frame
        self._error = error

    def candles(self, start, end, period):
        if self._error is not None:
            raise self._error
        return self._frame


def _candles_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(["2026-11-09 07:00", "2026-11-10 07:00"]), name="begin")
    return pd.DataFrame({"open": [1.0, 2.0], "close": [1.5, 2.5]}, index=index)


def _patch_tickers(monkeypatch, frames_by_symbol):
    def factory(symbol, *args, **kwargs):
        spec = frames_by_symbol[symbol]
        if isinstance(spec, Exception):
            return FakeTicker(symbol, error=spec)
        return FakeTicker(symbol, frame=spec)

    monkeypatch.setattr(moex_source, "Ticker", factory)


def test_fetch_daily_prices_maps_columns(monkeypatch):
    _patch_tickers(monkeypatch, {"SBER": _candles_frame()})

    result = moex_source.fetch_daily_prices(("SBER",), date(2026, 11, 9), date(2026, 11, 10))

    assert list(result.columns) == ["price_date", "ticker", "price"]
    assert result["ticker"].tolist() == ["SBER", "SBER"]
    assert result["price"].tolist() == [1.5, 2.5]


def test_fetch_daily_prices_raises_when_all_tickers_failed(monkeypatch):
    _patch_tickers(monkeypatch, {"SBER": RuntimeError("boom"), "GAZP": RuntimeError("boom")})

    with pytest.raises(RuntimeError, match="ни по одному тикеру"):
        moex_source.fetch_daily_prices(("SBER", "GAZP"), date(2026, 11, 9), date(2026, 11, 10))


def test_fetch_daily_prices_returns_partial_data_when_some_tickers_failed(monkeypatch):
    _patch_tickers(monkeypatch, {"SBER": _candles_frame(), "GAZP": RuntimeError("boom")})

    result = moex_source.fetch_daily_prices(("SBER", "GAZP"), date(2026, 11, 9), date(2026, 11, 10))

    assert set(result["ticker"]) == {"SBER"}


def test_fetch_daily_prices_returns_empty_frame_without_data_and_without_errors(monkeypatch):
    _patch_tickers(monkeypatch, {"SBER": pd.DataFrame()})

    result = moex_source.fetch_daily_prices(("SBER",), date(2026, 11, 9), date(2026, 11, 10))

    assert result.empty
    assert list(result.columns) == ["price_date", "ticker", "price"]
