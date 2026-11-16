# Образ Airflow с зависимостями DAG варианта 12
FROM apache/airflow:2.5.0-python3.8

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.8.txt"
