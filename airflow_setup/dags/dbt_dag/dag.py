from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# Имя DAG-а-источника
SOURCE_DAG_PRICES = "daily_prices_loader"

# dbt-postgres ставится в образ Airflow (см. requirements.txt), а проект analytics
# примонтирован томом в /opt/airflow/analytics, поэтому отдельный контейнер не нужен
DBT_PROJECT_DIR = "/opt/airflow/analytics"


def dbt_command(subcommand: str) -> str:
    """Собирает команду dbt для запуска внутри контейнера Airflow."""
    return (
        f"dbt {subcommand} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROJECT_DIR}"
    )


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
    1. Ждёт успешного завершения `daily_prices_loader`.
    2. Запускает `dbt run` и `dbt test` в контейнере Airflow (dbt установлен в образ).

    Зависимости:
    - Данные должны быть загружены до запуска dbt.
    - Использует `ExternalTaskSensor` для ожидания.
    """,
) as dag:

    # --- Ждём завершения DAG-а загрузки цен ---
    wait_for_prices = ExternalTaskSensor(
        task_id="wait_for_prices_dag",
        external_dag_id=SOURCE_DAG_PRICES,
        external_task_id=None,  # Ждём весь DAG
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
        bash_command=dbt_command("run"),
        # append_env=True нужен, чтобы задача унаследовала DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_command("test"),
        append_env=True,
    )

    # Порядок выполнения
    wait_for_prices >> dbt_run >> dbt_test
