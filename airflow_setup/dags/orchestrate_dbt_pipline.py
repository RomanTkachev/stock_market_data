from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# Имена исходных DAG-ов из твоих файлов
SOURCE_DAG_MOEX = "daily_moex_data_loader"
SOURCE_DAG_YFINANCE = "daily_nyse_data_loader"

with DAG(
    dag_id="run_dbt_after_sources",
    start_date=pendulum.datetime(2025, 10, 22, tz="Europe/Moscow"),
    schedule="0 9 * * *",  # Запуск в 9:00 — после 8:00 (когда source-DAG'и)
    catchup=False,
    tags=["dbt", "orchestration", "analytics", "market_data"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    doc_md="""
    ### DAG: Запуск dbt после загрузки данных

    Этот DAG:
    1. Ждёт успешного завершения:
       - `daily_moex_data_loader`
       - `daily_nyse_data_loader`
    2. Запускает `dbt run` в контейнере Airflow.
    
    Зависимости:
    - Данные должны быть загружены до запуска dbt.
    - Использует `ExternalTaskSensor` для ожидания.
    """,
) as dag:
    
    # --- Ждём завершения MOEX DAG ---
    wait_for_moex = ExternalTaskSensor(
        task_id="wait_for_moex_dag",
        external_dag_id=SOURCE_DAG_MOEX,
        external_task_id=None,  # Ждём весь DAG
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=pendulum.duration(seconds=30),
        timeout=pendulum.duration(hours=1),
        execution_date_fn=lambda dt: dt.in_timezone("Europe/Moscow").subtract(days=1).replace(hour=8, minute=0, second=0, microsecond=0),
        soft_fail=False,
    )

     # --- Ждём завершения yfinance DAG ---
    wait_for_yfinance = ExternalTaskSensor(
        task_id="wait_for_yfinance_dag",
        external_dag_id=SOURCE_DAG_YFINANCE,
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=pendulum.duration(seconds=30),
        timeout=pendulum.duration(hours=1),
        execution_date_fn=lambda dt: dt.in_timezone("Europe/Moscow").subtract(days=1).replace(hour=8, minute=0, second=0, microsecond=0),
        soft_fail=False,
    )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
      docker run --platform linux/amd64 --rm \
        -v /Users/notbad/Documents/Projects/finance_project/analytics:/app \
        -w /app \
        -e DBT_PROFILES_DIR=/app \
        ghcr.io/dbt-labs/dbt-postgres:1.9.0 \
        run
    """,
    )

    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
      docker run --platform linux/amd64 --rm \
        -v /Users/notbad/Documents/Projects/finance_project/analytics:/app \
        -w /app \
        -e DBT_PROFILES_DIR=/app \
        ghcr.io/dbt-labs/dbt-postgres:1.9.0 \
        test
    """,
    )

    # Порядок выполнения
    [wait_for_moex, wait_for_yfinance] >> dbt_run >> dbt_test