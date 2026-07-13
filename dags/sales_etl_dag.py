"""Daily sales ETL DAG: extract from source, transform, and load into the warehouse layer."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.extract import extract_sales_data
from tasks.load import load_sales_data
from tasks.transform import transform_sales_data

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**context):
    records = extract_sales_data()
    context["ti"].xcom_push(key="raw_records", value=records)


def _transform(**context):
    raw_records = context["ti"].xcom_pull(key="raw_records", task_ids="extract")
    transformed = transform_sales_data(raw_records)
    context["ti"].xcom_push(key="transformed_records", value=transformed)


def _load(**context):
    transformed_records = context["ti"].xcom_pull(key="transformed_records", task_ids="transform")
    num_loaded = load_sales_data(transformed_records)
    print(f"Loaded {num_loaded} records.")


with DAG(
    dag_id="sales_etl_daily",
    description="Extract, transform, and load daily sales data.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "sales", "demo"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    load = PythonOperator(task_id="load", python_callable=_load)

    extract >> transform >> load
