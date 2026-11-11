FROM apache/airflow:3.1.0
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# CLI dbt запускают DAG-и, поэтому кладём его в общий PATH независимо от HOME при сборке
RUN ln -sf "$(python -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')/dbt" /usr/local/bin/dbt

# падаем на сборке, если CLI недоступен
RUN dbt --version

