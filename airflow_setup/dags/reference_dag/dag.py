from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.config import DEFAULT_LOOKBACK_DAYS
from etl.pipelines import load_actions_interval, load_instruments


def load_actions(**context) -> dict:
    """Дивиденды и сплиты за недельное окно запуска."""
    return load_actions_interval(
        interval_start=context["data_interval_start"],
        interval_end=context["data_interval_end"],
        lookback_days=DEFAULT_LOOKBACK_DAYS,
    )


def update_instruments() -> dict:
    """Обновляет справочник инструментов (метаданные из Yahoo Finance)."""
    return load_instruments()


with DAG(
    dag_id="weekly_reference_loader",
    start_date=pendulum.datetime(2025, 10, 20, tz="Europe/Moscow"),
    schedule="0 7 * * 1",  # понедельник, 07:00 MSK — до загрузки цен
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=15),
    },
    tags=["market_data", "reference", "yfinance"],
    doc_md="""
    ### Справочные данные: дивиденды, сплиты, метаданные инструментов

    Запускается раз в неделю перед загрузкой цен:

    1. `load_actions` — дивиденды и сплиты за недельное окно (`etl.pipelines.load_actions_interval`).
       Для пустых таблиц выполняется первичная загрузка всей истории.
    2. `update_instruments` — метаданные справочника: класс актива, валюта, биржа,
       отрасль, капитализация (`etl.pipelines.load_instruments`).

    Оба шага идемпотентны (upsert по ключам), поэтому повторный запуск безопасен.
    """,
) as dag:
    PythonOperator(
        task_id="load_actions",
        python_callable=load_actions,
    ) >> PythonOperator(
        task_id="update_instruments",
        python_callable=update_instruments,
    )
