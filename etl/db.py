"""Доступ к PostgreSQL: последние даты по тикерам и идемпотентная запись цен."""

from __future__ import annotations

import os
from datetime import date
from typing import Iterable

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from etl.config import (
    DIVIDENDS_TABLE,
    INSTRUMENTS_TABLE,
    PRICES_TABLE,
    SPLITS_TABLE,
    SCHEMA,
)

DEFAULT_BATCH_SIZE = 5_000

_metadata = MetaData()

# Поля справочника, которые обновляются при повторной загрузке метаданных
INSTRUMENT_UPDATE_COLUMNS = (
    "name",
    "instrument_type",
    "currency",
    "exchange",
    "sector",
    "industry",
    "market_cap",
    "updated_at",
)

PRICES = Table(
    PRICES_TABLE,
    _metadata,
    Column("price_date", Date, nullable=False),
    Column("ticker", String(20), nullable=False),
    Column("open", Numeric(18, 6)),
    Column("high", Numeric(18, 6)),
    Column("low", Numeric(18, 6)),
    Column("close", Numeric(18, 6), nullable=False),
    Column("adj_close", Numeric(18, 6)),
    Column("volume", BigInteger),
    schema=SCHEMA,
)

DIVIDENDS = Table(
    DIVIDENDS_TABLE,
    _metadata,
    Column("ticker", String(20), nullable=False),
    Column("ex_date", Date, nullable=False),
    Column("amount", Numeric(18, 6), nullable=False),
    schema=SCHEMA,
)

SPLITS = Table(
    SPLITS_TABLE,
    _metadata,
    Column("ticker", String(20), nullable=False),
    Column("split_date", Date, nullable=False),
    Column("ratio", Numeric(18, 6), nullable=False),
    schema=SCHEMA,
)

INSTRUMENTS = Table(
    INSTRUMENTS_TABLE,
    _metadata,
    Column("instrument_id", BigInteger, primary_key=True),
    Column("ticker", String(20), nullable=False),
    Column("name", String(200)),
    Column("instrument_type", String(20)),
    Column("currency", String(10)),
    Column("exchange", String(20)),
    Column("sector", String(60)),
    Column("industry", String(80)),
    Column("market_cap", Numeric(24, 2)),
    Column("updated_at", DateTime(timezone=True)),
    schema=SCHEMA,
)


def get_engine(database_url: str | None = None) -> Engine:
    """Создаёт движок SQLAlchemy по DATABASE_URL."""
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL не настроен")
    return create_engine(url)


def get_last_dates(engine: Engine, tickers: Iterable[str]) -> dict[str, date]:
    """Возвращает последнюю загруженную дату цен по каждому тикеру."""
    ticker_list = [str(t) for t in tickers]
    if not ticker_list:
        return {}

    stmt = (
        select(PRICES.c.ticker, func.max(PRICES.c.price_date))
        .where(PRICES.c.ticker.in_(ticker_list))
        .group_by(PRICES.c.ticker)
    )
    with engine.connect() as conn:
        return {row[0]: row[1] for row in conn.execute(stmt)}


def has_history(engine: Engine, tickers: Iterable[str]) -> bool:
    """Есть ли в таблице цен хоть какие-то данные по указанным тикерам."""
    return bool(get_last_dates(engine, tickers))


def count_rows(engine: Engine, table: Table) -> int:
    """Число строк в таблице (используется для решения о первичной загрузке)."""
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(table)).scalar())


def upsert_prices(engine: Engine, df: pd.DataFrame, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Идемпотентно пишет цены: конфликты по (ticker, price_date) игнорируются.

    Возвращает число реально вставленных строк (0 при повторном запуске за ту же дату).
    """
    return _upsert(engine, PRICES, _price_records(df), ["ticker", "price_date"], batch_size)


def upsert_dividends(engine: Engine, df: pd.DataFrame, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Идемпотентно пишет дивиденды по ключу (ticker, ex_date)."""
    records = [
        {
            "ticker": str(row.ticker),
            "ex_date": pd.Timestamp(row.ex_date).date(),
            "amount": _number(row.amount),
        }
        for row in df.itertuples(index=False)
    ]
    return _upsert(engine, DIVIDENDS, records, ["ticker", "ex_date"], batch_size)


def upsert_splits(engine: Engine, df: pd.DataFrame, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Идемпотентно пишет сплиты по ключу (ticker, split_date)."""
    records = [
        {
            "ticker": str(row.ticker),
            "split_date": pd.Timestamp(row.split_date).date(),
            "ratio": _number(row.ratio),
        }
        for row in df.itertuples(index=False)
    ]
    return _upsert(engine, SPLITS, records, ["ticker", "split_date"], batch_size)


def upsert_instruments(engine: Engine, rows: Sequence[dict], batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Обновляет справочник инструментов (upsert по тикеру)."""
    return _upsert(
        engine,
        INSTRUMENTS,
        list(rows),
        ["ticker"],
        batch_size,
        update_columns=INSTRUMENT_UPDATE_COLUMNS,
    )


def _upsert(
    engine: Engine,
    table: Table,
    records: Sequence[dict],
    index_elements: list[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    update_columns: Sequence[str] = (),
) -> int:
    if not records:
        return 0

    written = 0
    with engine.begin() as conn:
        for start in range(0, len(records), batch_size):
            chunk = records[start : start + batch_size]
            stmt = pg_insert(table).values(chunk)
            if update_columns:
                stmt = stmt.on_conflict_do_update(
                    index_elements=index_elements,
                    set_={column: stmt.excluded[column] for column in update_columns},
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=index_elements).returning(
                    table.c.ticker
                )

            result = conn.execute(stmt)
            written += len(chunk) if update_columns else len(result.fetchall())

    return written


def _price_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    records = []
    for row in df.itertuples(index=False):
        records.append(
            {
                "price_date": pd.Timestamp(row.price_date).date(),
                "ticker": str(row.ticker),
                "open": _number(getattr(row, "open", None)),
                "high": _number(getattr(row, "high", None)),
                "low": _number(getattr(row, "low", None)),
                "close": _number(getattr(row, "close", None)),
                "adj_close": _number(getattr(row, "adj_close", None)),
                "volume": _integer(getattr(row, "volume", None)),
            }
        )
    return records


def _number(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _integer(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)
