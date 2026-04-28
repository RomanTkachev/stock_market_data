from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Импортируем нашу основную функцию из соседнего файла
# Airflow автоматически добавляет папку dags в sys.path, поэтому импорт сработает

from scripts.yfinance_loader import load_yfinance_data_to_db


with DAG(
    dag_id="daily_nyse_data_loader",
    start_date=pendulum.datetime(2025, 10, 22, tz="Europe/Moscow"),
    schedule="0 8 * * *",  # Запускать каждый день в 8:00 утра по Москве
    catchup=False,
    doc_md="""
    ### DAG для загрузки рыночных данных

    Этот DAG ежедневно выполняет следующие шаги:
    1. Подключается к базе данных.
    2. Загружает последние котировки с yfinance.
    3. Добавляет только новые данные в таблицы `prices_yfinance`
    """,
    
    tags=["market_data", "yfinance"],
) as dag:
    
    load_data_task = PythonOperator(
        task_id="load_new_market_data_to_postgres",
        python_callable=load_yfinance_data_to_db,  
    )