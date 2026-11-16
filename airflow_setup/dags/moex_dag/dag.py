from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.config import DEFAULT_LOOKBACK_DAYS
from etl.pipelines import load_moex_interval


def load_interval(**context) -> dict:
    """Загружает окно данных, привязанное к логической дате запуска."""
    return load_moex_interval(
        interval_start=context["data_interval_start"],
        interval_end=context["data_interval_end"],
        lookback_days=DEFAULT_LOOKBACK_DAYS,
    )


with DAG(
    dag_id="daily_moex_data_loader",
    start_date=pendulum.datetime(2025, 10, 22, tz="Europe/Moscow"),
    schedule="0 8 * * *",  # каждый день в 8:00 по Москве
    catchup=True,  # пропущенные интервалы догоняются (backfill)
    max_active_runs=1,  # защита от параллельных запусков и дублей
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["market_data", "moex"],
    doc_md="""
    ### Загрузка дневных свечей MOEX (Algopack)

    Окно загрузки привязано к `data_interval` запуска, верхняя граница
    передаётся в moexalgo как включительная.

    - `catchup=True` / `max_active_runs=1` — догоняем пропуски без параллельных запусков;
    - запись идемпотентна (`ON CONFLICT (ticker, price_date) DO NOTHING`);
    - для пустой таблицы грузится история с `HISTORY_START_DATE` (bootstrap);
    - требуется переменная окружения `MOEX_API_TOKEN`.

    Логика лежит в пакете `etl` (`etl.pipelines.load_moex_interval`).
    """,
) as dag:
    PythonOperator(
        task_id="load_moex_prices",
        python_callable=load_interval,
    )
