from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.moex_loader import load_moex_data_to_db

with DAG(
    dag_id="daily_moex_data_loader",
    start_date=pendulum.datetime(2025, 10, 22, tz="Europe/Moscow"),
    schedule="0 8 * * *",  # Запускать каждый день в 8:00 утра по Москве
    catchup=False,
    doc_md="""
    ### DAG для загрузки рыночных данных

    Этот DAG ежедневно выполняет следующие шаги:
    1. Подключается к базе данных.
    2. Загружает последние котировки с московской фондовой биржи.
    3. Добавляет только новые данные в таблицы 'prices'.
    """,
    
    tags=["market_data", "moex"],
) as dag:
    
    load_data_task = PythonOperator(
        task_id="load_new_moex_market_data_to_postgres",
        python_callable=load_moex_data_to_db, 
    )