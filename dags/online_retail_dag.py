"""
DAG варианта 12 "Анализ онлайн-продаж".

Конвейер: UCI Online Retail (xlsx) -> RAW (raw_online_retail) -> STG (stg_online_retail
+ журнал отбраковки stg_online_retail_rejects) -> витрина online_retail_datamart (VIEW)
-> проверка сходимости. Superset подключается только к витрине.
"""
import csv
import hashlib
import io
import logging
import os
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

CONN_ID = "analytics_postgres"
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DAGS_DIR, "data")
SOURCE_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
XLSX_NAME = "Online Retail.xlsx"
EXPECTED_SOURCE_ROWS = 541909  # число записей по карточке набора на UCI

RAW_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
]

log = logging.getLogger(__name__)


def _quality_log(cur, step, rule, rows, revenue=None, comment=None):
    cur.execute(
        """
        INSERT INTO etl_quality_log (step, rule, rows_affected, revenue_affected, comment)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (step, rule, rows, revenue, comment),
    )


def _ensure_log_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_quality_log (
            log_id           BIGSERIAL PRIMARY KEY,
            logged_at        TIMESTAMP NOT NULL DEFAULT now(),
            step             TEXT      NOT NULL,
            rule             TEXT      NOT NULL,
            rows_affected    BIGINT,
            revenue_affected NUMERIC(14, 3),
            comment          TEXT
        )
        """
    )


def download_dataset(**_):
    """Скачивает архив UCI и распаковывает xlsx. Если файл уже есть, повторно не качает."""
    os.makedirs(DATA_DIR, exist_ok=True)
    xlsx_path = os.path.join(DATA_DIR, XLSX_NAME)

    if os.path.exists(xlsx_path) and os.path.getsize(xlsx_path) > 0:
        log.info("Файл уже загружен ранее: %s", xlsx_path)
    else:
        zip_path = os.path.join(DATA_DIR, "online_retail.zip")
        log.info("Скачивание %s", SOURCE_URL)
        with requests.get(SOURCE_URL, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extract(XLSX_NAME, DATA_DIR)
        os.remove(zip_path)

    sha = hashlib.sha256()
    with open(xlsx_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    log.info("Размер файла: %s байт, sha256: %s", os.path.getsize(xlsx_path), sha.hexdigest())
    return xlsx_path


def load_raw(**context):
    """RAW-слой: копия источника без изменений, все поля текстом."""
    xlsx_path = context["ti"].xcom_pull(task_ids="download_dataset")
    df = pd.read_excel(xlsx_path, dtype=str, engine="openpyxl")
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("В файле нет ожидаемых столбцов: {}".format(missing))
    df = df[RAW_COLUMNS]
    df.insert(0, "source_row_num", range(1, len(df) + 1))
    log.info("Прочитано строк из источника: %s", len(df))
    if len(df) != EXPECTED_SOURCE_ROWS:
        log.warning("Число строк отличается от карточки UCI (%s)", EXPECTED_SOURCE_ROWS)

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL, na_rep="")
    buf.seek(0)

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        _ensure_log_table(cur)
        cur.execute("DROP VIEW IF EXISTS online_retail_datamart")
        cur.execute("DROP TABLE IF EXISTS raw_online_retail")
        cur.execute(
            """
            CREATE TABLE raw_online_retail (
                source_row_num INTEGER PRIMARY KEY,
                invoice_no     TEXT,
                stock_code     TEXT,
                description    TEXT,
                quantity       TEXT,
                invoice_date   TEXT,
                unit_price     TEXT,
                customer_id    TEXT,
                country        TEXT,
                loaded_at      TIMESTAMP NOT NULL DEFAULT now()
            )
            """
        )
        cur.copy_expert(
            """
            COPY raw_online_retail (source_row_num, invoice_no, stock_code, description,
                                    quantity, invoice_date, unit_price, customer_id, country)
            FROM STDIN WITH (FORMAT csv, NULL '')
            """,
            buf,
        )
        cur.execute("SELECT count(*) FROM raw_online_retail")
        loaded = cur.fetchone()[0]
        _quality_log(cur, "load_raw", "rows_loaded", loaded, comment=os.path.basename(xlsx_path))
    conn.close()
    if loaded != len(df):
        raise ValueError("В RAW загружено {} строк вместо {}".format(loaded, len(df)))
    log.info("RAW: загружено %s строк", loaded)


def validate_datamart(**_):
    """Сходимость: RAW = STG + отбраковка; итог витрины = сумма по странам и по датам."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    q = hook.get_first
    raw_rows = q("SELECT count(*) FROM raw_online_retail")[0]
    stg_rows = q("SELECT count(*) FROM stg_online_retail")[0]
    rej_rows = q("SELECT count(*) FROM stg_online_retail_rejects")[0]
    dm_rows = q("SELECT count(*) FROM online_retail_datamart")[0]
    total = q("SELECT sum(revenue) FROM online_retail_datamart")[0]
    by_country = q(
        "SELECT sum(r) FROM (SELECT country, sum(revenue) r FROM online_retail_datamart GROUP BY country) t"
    )[0]
    by_date = q(
        "SELECT sum(r) FROM (SELECT invoice_date, sum(revenue) r FROM online_retail_datamart GROUP BY invoice_date) t"
    )[0]
    orders = q("SELECT count(DISTINCT order_no) FROM online_retail_datamart")[0]
    orders_by_country = q(
        "SELECT sum(o) FROM (SELECT country, count(DISTINCT order_no) o FROM online_retail_datamart GROUP BY country) t"
    )[0]
    nulls = q(
        """SELECT count(*) FROM online_retail_datamart
           WHERE invoice_date IS NULL OR country IS NULL OR stock_code IS NULL
              OR description IS NULL OR revenue IS NULL"""
    )[0]

    checks = {
        "raw_equals_stg_plus_rejects": raw_rows == stg_rows + rej_rows,
        "datamart_equals_stg": dm_rows == stg_rows,
        "total_equals_sum_by_country": total == by_country,
        "total_equals_sum_by_date": total == by_date,
        "orders_equal_sum_by_country": orders == orders_by_country,
        "no_nulls_in_required_fields": nulls == 0,
    }
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        _quality_log(cur, "validate", "raw_rows", raw_rows)
        _quality_log(cur, "validate", "stg_rows", stg_rows)
        _quality_log(cur, "validate", "rejected_rows", rej_rows)
        _quality_log(cur, "validate", "datamart_rows", dm_rows, total)
        _quality_log(cur, "validate", "orders_count", orders)
        for name, ok in checks.items():
            _quality_log(cur, "validate", name, None, comment="OK" if ok else "FAILED")
    conn.close()

    log.info("RAW=%s STG=%s REJECTS=%s DATAMART=%s REVENUE=%s ORDERS=%s",
             raw_rows, stg_rows, rej_rows, dm_rows, total, orders)
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError("Не пройдены проверки: {}".format(failed))


default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="online_retail_analysis",
    description="Вариант 12: анализ онлайн-продаж (UCI Online Retail)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    template_searchpath=[DAGS_DIR],
    tags=["lpw_03", "variant_12", "online_retail"],
) as dag:

    t_download = PythonOperator(task_id="download_dataset", python_callable=download_dataset)

    t_raw = PythonOperator(task_id="load_raw", python_callable=load_raw)

    t_stg = PostgresOperator(
        task_id="transform_stg",
        postgres_conn_id=CONN_ID,
        sql="sql/stg_online_retail.sql",
    )

    t_datamart = PostgresOperator(
        task_id="create_datamart",
        postgres_conn_id=CONN_ID,
        sql="datamart_variant_12.sql",
    )

    t_validate = PythonOperator(task_id="validate_datamart", python_callable=validate_datamart)

    t_download >> t_raw >> t_stg >> t_datamart >> t_validate
