"""Доступ к PostgreSQL: последние даты по тикерам и идемпотентная запись цен."""

from __future__ import annotations

import os
from datetime import date
from typing import Iterable

import pandas as pd
from sqlalchemy import Column, Date, MetaData, Numeric, String, Table, create_engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from etl.config import PRICES_MOEX_TABLE, PRICES_YFINANCE_TABLE, SCHEMA

DEFAULT_BATCH_SIZE = 5_000

_metadata = MetaData()


def _make_prices_table(name: str) -> Table:
    return Table(
        name,
        _metadata,
        Column("price_date", Date, nullable=False),
        Column("ticker", String(20), nullable=False),
        Column("price", Numeric(18, 6)),
        schema=SCHEMA,
    )


PRICES_TABLES: dict[str, Table] = {
    name: _make_prices_table(name) for name in (PRICES_MOEX_TABLE, PRICES_YFINANCE_TABLE)
}


def prices_table(table_name: str) -> Table:
    try:
        return PRICES_TABLES[table_name]
    except KeyError as exc:
        raise ValueError(f"Неизвестная таблица цен: {table_name}") from exc


def get_engine(database_url: str | None = None) -> Engine:
    """Создаёт движок SQLAlchemy по DATABASE_URL."""
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL не настроен")
    return create_engine(url)


def get_last_dates(engine: Engine, table_name: str, tickers: Iterable[str]) -> dict[str, date]:
    """Возвращает последнюю загруженную дату по каждому тикеру."""
    ticker_list = [str(t) for t in tickers]
    if not ticker_list:
        return {}

    table = prices_table(table_name)
    stmt = (
        select(table.c.ticker, func.max(table.c.price_date))
        .where(table.c.ticker.in_(ticker_list))
        .group_by(table.c.ticker)
    )
    with engine.connect() as conn:
        return {row[0]: row[1] for row in conn.execute(stmt)}


def has_history(engine: Engine, table_name: str, tickers: Iterable[str]) -> bool:
    """Есть ли в таблице хоть какие-то данные по указанным тикерам."""
    return bool(get_last_dates(engine, table_name, tickers))


def upsert_prices(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Идемпотентно пишет цены: конфликты по (ticker, price_date) игнорируются.

    Возвращает число реально вставленных строк (0 при повторном запуске за ту же дату).
    Требует уникальный индекс ux_<table>_ticker_date (см. infra/initdb и infra/migrations).
    """
    if df.empty:
        return 0

    table = prices_table(table_name)
    records = _to_records(df)
    inserted = 0

    with engine.begin() as conn:
        for start in range(0, len(records), batch_size):
            chunk = records[start : start + batch_size]
            stmt = (
                pg_insert(table)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["ticker", "price_date"])
                .returning(table.c.ticker)
            )
            inserted += len(conn.execute(stmt).fetchall())

    return inserted


def _to_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.itertuples(index=False):
        records.append(
            {
                "price_date": pd.Timestamp(row.price_date).date(),
                "ticker": str(row.ticker),
                "price": None if pd.isna(row.price) else float(row.price),
            }
        )
    return records
