# airflow-etl-demo

[![CI](https://github.com/aayushi-jha2018/airflow-etl-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/aayushi-jha2018/airflow-etl-demo/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


A small Airflow-orchestrated ETL demo: extract daily sales data, transform and clean it, and load it into a warehouse-ready file. This mirrors the batch pipeline patterns described in my [portfolio](https://github.com/aayushi-jha2018/portfolio) (Airflow, AWS Glue/EMR), scaled down to something you can run locally without cloud credentials.

## Architecture

```
data/raw_sales.csv
        |
        v
  extract_sales_data()      -- dags/tasks/extract.py
        |
        v
  transform_sales_data()    -- dags/tasks/transform.py   (typing, cleanup, totals)
        |
        v
  load_sales_data()         -- dags/tasks/load.py         (writes data/processed_sales.csv)
```

Orchestrated by the `sales_etl_daily` DAG in `dags/sales_etl_dag.py`, which chains three `PythonOperator` tasks (`extract >> transform >> load`) and passes data between them via XCom.

## Project structure

```
airflow-etl-demo/
|-- data/
|   `-- raw_sales.csv          # sample source data
|-- dags/
|   |-- sales_etl_dag.py       # DAG definition
|   `-- tasks/
|       |-- extract.py
|       |-- transform.py
|       `-- load.py
`-- requirements.txt
```

## Running this project

```bash
pip install -r requirements.txt
export AIRFLOW_HOME=$(pwd)/.airflow
airflow db init
airflow dags list          # confirms sales_etl_daily is discovered
airflow tasks test sales_etl_daily extract 2024-01-01
airflow tasks test sales_etl_daily transform 2024-01-01
airflow tasks test sales_etl_daily load 2024-01-01
```

Point Airflow at this repo's `dags/` folder (via `AIRFLOW_HOME`'s `airflow.cfg`, `dags_folder` setting) so it picks up `sales_etl_dag.py`.

## What this demonstrates

- **Task decomposition**: extract, transform, and load are separate, independently testable Python functions, not one monolithic script.
- **DAG dependency management**: `extract >> transform >> load` makes the pipeline order explicit and lets Airflow retry individual steps on failure.
- **Data passing via XCom**: each task pushes its output for the next task to pull, the same pattern used for small-to-medium payloads in production DAGs.
- **Retries and scheduling**: `default_args` configures automatic retries with backoff, and the DAG runs on a daily schedule with `catchup=False`.

In production (see my portfolio), the extract step calls source APIs or databases and the load step writes to a warehouse like Snowflake or Redshift instead of a local CSV.

## License

MIT — feel free to reuse this as a starting point.
