FROM apache/airflow:3.1.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# dbt CLI ставится в /home/airflow/.local/bin, который уже присутствует в PATH образа;
# проверяем, что команда доступна (иначе сборка упадёт сразу)
RUN dbt --version

# Пакет с ETL-логикой, который импортируют DAG-и.
# В PYTHONPATH явно добавлен user-site Airflow: контейнеры в compose запускаются под root,
# а root по умолчанию не видит ~/.local (обычно его добавляет /entrypoint).
COPY etl /opt/python/etl
ENV PYTHONPATH="/opt/python:/home/airflow/.local/lib/python3.12/site-packages"

