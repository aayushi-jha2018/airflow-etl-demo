"""Data-quality validation task.

Uses Great Expectations to check transformed sales records against a set of
expectations before they reach the load step, so malformed data fails the
Airflow task (and therefore the DAG run) instead of silently landing in the
output file.
"""

from typing import Dict, List

import great_expectations as gx
import pandas as pd


class DataQualityError(Exception):
    """Raised when transformed sales data fails a data-quality expectation."""


EXPECTED_REGIONS = ["North", "South", "East", "West"]


def validate_sales_data(records: List[Dict[str, object]]) -> bool:
    """Validate transformed records with Great Expectations.

    Raises DataQualityError if any expectation fails. Returns True if every
    record passes every expectation.
    """
    if not records:
        raise DataQualityError("No records to validate: transform step produced an empty result.")

    df = pd.DataFrame(records)

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("sales_data_source")
    data_asset = data_source.add_dataframe_asset(name="transformed_sales")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("transformed_sales_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
        gx.expectations.ExpectColumnValuesToBeInSet(column="region", value_set=EXPECTED_REGIONS),
        gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1),
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0),
    ]

    failed_columns = []
    for expectation in expectations:
        result = batch.validate(expectation)
        if not result.success:
            failed_columns.append(expectation.column)

    if failed_columns:
        raise DataQualityError(f"Data quality checks failed for columns: {failed_columns}")

    return True
