"""Daily sales ETL DAG: extract from source, transform, validate, and load into the warehouse layer."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.extract import extract_sales_data
from tasks.load import load_sales_data
from tasks.transform import transform_sales_data
from tasks.validate import validate_sales_data

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


def _validate(**context):
    transformed_records = context["ti"].xcom_pull(key="transformed_records", task_ids="transform")
    validate_sales_data(transformed_records)
    context["ti"].xcom_push(key="validated_records", value=transformed_records)


def _load(**context):
    validated_records = context["ti"].xcom_pull(key="validated_records", task_ids="validate")
    num_loaded = load_sales_data(validated_records)
    print(f"Loaded {num_loaded} records.")


with DAG(
    dag_id="sales_etl_daily",
    description="Extract, transform, validate, and load daily sales data.",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "sales", "demo"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    validate = PythonOperator(task_id="validate", python_callable=_validate)
    load = PythonOperator(task_id="load", python_callable=_load)

    extract >> transform >> validate >> load
