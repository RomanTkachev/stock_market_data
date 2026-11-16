from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.config import DEFAULT_LOOKBACK_DAYS
from etl.pipelines import load_prices_interval


def load_interval(**context) -> dict:
    """Загружает окно цен, привязанное к логической дате запуска."""
    return load_prices_interval(
        interval_start=context["data_interval_start"],
        interval_end=context["data_interval_end"],
        lookback_days=DEFAULT_LOOKBACK_DAYS,
    )


with DAG(
    dag_id="daily_prices_loader",
    start_date=pendulum.datetime(2025, 10, 22, tz="Europe/Moscow"),
    schedule="0 8 * * *",  # каждый день в 8:00 по Москве
    catchup=True,  # пропущенные интервалы догоняются (backfill)
    max_active_runs=1,  # защита от параллельных запусков и дублей
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["market_data", "prices", "yfinance"],
    doc_md="""
    ### Загрузка рыночных данных (Yahoo Finance)

    Задача загружает дневные свечи OHLCV за окно, привязанное к `data_interval`:
    `[data_interval_start - LOADER_LOOKBACK_DAYS, data_interval_end)`.

    - `catchup=True` — пропущенные дни догоняются запусками за прошедшие интервалы;
    - `max_active_runs=1` — запуски не пересекаются;
    - запись идемпотентна: `INSERT ... ON CONFLICT (ticker, price_date) DO NOTHING`;
    - если в таблице ещё нет данных, грузится история с `HISTORY_START_DATE` (bootstrap).

    Вселенная инструментов (акции, ETF, крипта, валюты, индексы, сырьё) задаётся
    в `etl/config.py` (`UNIVERSE`). Логика — `etl.pipelines.load_prices_interval`.
    """,
) as dag:
    PythonOperator(
        task_id="load_prices",
        python_callable=load_interval,
    )
