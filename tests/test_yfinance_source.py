import pandas as pd
import pytest

from etl.yfinance_source import to_long_prices


def _wide_frame() -> pd.DataFrame:
    index = pd.to_datetime(["2026-11-09", "2026-11-10"])
    columns = pd.MultiIndex.from_product([["AAPL", "MSFT"], ["Close", "Volume"]])
    return pd.DataFrame([[1.0, 10, 3.0, 30], [2.0, 20, 4.0, 40]], index=index, columns=columns)


def test_to_long_prices_converts_multiindex_frame():
    long = to_long_prices(_wide_frame(), ["AAPL", "MSFT"])

    assert list(long.columns) == ["price_date", "ticker", "price"]
    assert len(long) == 4
    assert set(long["ticker"]) == {"AAPL", "MSFT"}
    mask = (long["ticker"] == "MSFT") & (long["price_date"] == pd.Timestamp("2026-11-10"))
    assert long.loc[mask, "price"].item() == 4.0


def test_to_long_prices_drops_rows_without_price():
    frame = _wide_frame()
    frame.loc[:, ("AAPL", "Close")] = [1.0, float("nan")]

    long = to_long_prices(frame, ["AAPL", "MSFT"])

    assert len(long) == 3


def test_to_long_prices_supports_single_ticker_flat_frame():
    index = pd.to_datetime(["2026-11-09"])
    raw = pd.DataFrame({"Close": [10.0], "Volume": [100]}, index=index)

    long = to_long_prices(raw, ["AAPL"])

    assert long.to_dict("records") == [
        {"price_date": pd.Timestamp("2026-11-09"), "ticker": "AAPL", "price": 10.0}
    ]


def test_to_long_prices_requires_tickers_for_flat_frame():
    with pytest.raises(ValueError):
        to_long_prices(pd.DataFrame({"Close": [1.0]}), ())
