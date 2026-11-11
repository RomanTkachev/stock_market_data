FROM apache/airflow:3.1.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# dbt CLI ставится в /home/airflow/.local/bin, который уже присутствует в PATH образа;
# проверяем, что команда доступна (иначе сборка упадёт сразу)
RUN dbt --version

