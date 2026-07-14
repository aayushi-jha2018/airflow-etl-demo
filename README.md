# airflow-etl-demo

A small Airflow-orchestrated ETL demo: extract daily sales data, transform and clean it, validate it against a set of data-quality expectations, and load it into a warehouse-ready file. Scaled-down version of the batch pipeline patterns (Airflow, AWS Glue/EMR) described in my portfolio -- something you can actually run locally without cloud credentials.

## Run it

```
pip install -r requirements.txt
pytest tests/ -v                          # data-quality unit tests, no Airflow needed
export AIRFLOW_HOME=$(pwd)/.airflow
airflow db migrate
airflow dags list                          # confirms sales_etl_daily is discovered
airflow dags test sales_etl_daily 2024-01-01
```

Airflow needs to be pointed at this repo's `dags/` folder (the `dags_folder` setting in `AIRFLOW_HOME`'s `airflow.cfg`) to pick up `sales_etl_dag.py`. To debug a single task instead of the whole DAG, `airflow tasks test sales_etl_daily <extract|transform|validate|load> 2024-01-01` works, but it doesn't persist XCom to the metadata database -- so chaining separate `tasks test` calls won't pass data between tasks the way `dags test` does.

## The pipeline

```
data/raw_sales.csv
  -> extract_sales_data()    dags/tasks/extract.py
  -> transform_sales_data()  dags/tasks/transform.py   (typing, cleanup, totals)
  -> validate_sales_data()   dags/tasks/validate.py    (Great Expectations checks)
  -> load_sales_data()       dags/tasks/load.py        (writes data/processed_sales.csv)
```

The `sales_etl_daily` DAG chains these four as PythonOperator tasks (`extract >> transform >> validate >> load`) and passes data between them via XCom. If validation fails, the task raises and the whole DAG run fails -- bad data never reaches load.

## Data quality checks

Before data gets to the load step, `validate` checks that order_id is never null and is unique across the batch, region is one of North/South/East/West, quantity is at least 1, and total_amount is non-negative.

`tests/test_validate.py` proves this actually catches bad data, not just that it passes on good input -- it has dedicated cases for duplicate order IDs, invalid regions, and negative quantities, each asserted to raise `DataQualityError`.

## A real snag while building this

The first version of `validate.py` was written against a newer Great Expectations API than what ended up pinned in `requirements.txt`, so the validate task passed locally but failed in CI with an API-mismatch error that had nothing to do with the actual data-quality logic. Fixed by pinning the dependency version and rewriting `validate.py` against the API that version actually exposes. Small thing, but a good reminder that "works on my machine" and "works in CI" are two different claims.

## Layout

- `data/raw_sales.csv` -- sample source data
- `dags/sales_etl_dag.py` -- DAG definition
- `dags/tasks/extract.py`, `transform.py`, `validate.py`, `load.py`
- `tests/test_validate.py` -- unit tests, including bad-data cases
- `pytest.ini`, `requirements.txt`

## In production

As described in my portfolio, the extract step would call source APIs or databases, and load would write to a warehouse like Snowflake or Redshift instead of a local CSV. Retries with backoff and the daily schedule (`catchup=False`) are already configured in `default_args`, so that part carries over as-is.

MIT license.
