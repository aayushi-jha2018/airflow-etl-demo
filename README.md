# airflow-etl-demo

[![CI](https://github.com/aayushi-jha2018/airflow-etl-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/aayushi-jha2018/airflow-etl-demo/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A small Airflow-orchestrated ETL demo: extract daily sales data, transform and clean it, validate it against a set of data-quality expectations, and load it into a warehouse-ready file. This mirrors the batch pipeline patterns described in my portfolio (Airflow, AWS Glue/EMR), scaled down to something you can run locally without cloud credentials.

## Architecture

```
data/raw_sales.csv
        |
        v
extract_sales_data()  -- dags/tasks/extract.py
        |
        v
transform_sales_data() -- dags/tasks/transform.py (typing, cleanup, totals)
        |
        v
validate_sales_data()  -- dags/tasks/validate.py (Great Expectations checks)
        |
        v
load_sales_data()      -- dags/tasks/load.py (writes data/processed_sales.csv)
```

Orchestrated by the sales_etl_daily DAG in dags/sales_etl_dag.py, which chains four PythonOperator tasks (extract >> transform >> validate >> load) and passes data between them via XCom. If validation fails, the task raises and the DAG run fails -- bad data never reaches the load step.

## Data quality checks

The validate task runs the following expectations (via Great Expectations) against the transformed records before they're loaded:

- order_id is never null and is unique across the batch
- region is one of the four expected values (North/South/East/West)
- quantity is at least 1
- total_amount is non-negative

tests/test_validate.py proves this actually catches bad data -- not just that it passes on good input -- with dedicated cases for duplicate order IDs, invalid regions, and negative quantities, each asserted to raise DataQualityError.

## Project structure

```
airflow-etl-demo/
|-- data/
|   `-- raw_sales.csv       # sample source data
|-- dags/
|   |-- sales_etl_dag.py    # DAG definition
|   `-- tasks/
|       |-- extract.py
|       |-- transform.py
|       |-- validate.py     # Great Expectations data-quality checks
|       `-- load.py
|-- tests/
|   `-- test_validate.py    # unit tests, including bad-data cases
|-- pytest.ini
`-- requirements.txt
```

## Running this project

```
pip install -r requirements.txt
pytest tests/ -v                       # data-quality unit tests, no Airflow needed
export AIRFLOW_HOME=$(pwd)/.airflow
airflow db migrate
airflow dags list                      # confirms sales_etl_daily is discovered
airflow dags test sales_etl_daily 2024-01-01
```

Point Airflow at this repo's dags/ folder (via AIRFLOW_HOME's airflow.cfg, dags_folder setting) so it picks up sales_etl_dag.py.

To debug a single task in isolation, you can use airflow tasks test sales_etl_daily <extract|transform|validate|load> 2024-01-01. Note that this runs the task standalone and does not persist XCom to the metadata database, so chaining separate tasks test calls will not pass data between tasks the way dags test does.

## What this demonstrates

- **Task decomposition**: extract, transform, validate, and load are separate, independently testable Python functions, not one monolithic script.
- **DAG dependency management**: extract >> transform >> validate >> load makes the pipeline order explicit and lets Airflow retry individual steps on failure.
- **Data passing via XCom**: each task pushes its output for the next task to pull, the same pattern used for small-to-medium payloads in production DAGs.
- **Data quality as a pipeline gate, not an afterthought**: validation runs as its own task between transform and load, using a real expectations library, with tests proving it actually rejects bad data.
- **Retries and scheduling**: default_args configures automatic retries with backoff, and the DAG runs on a daily schedule with catchup=False.

In production (see my portfolio), the extract step calls source APIs or databases and the load step writes to a warehouse like Snowflake or Redshift instead of a local CSV.

## License

MIT -- feel free to reuse this as a starting point.
