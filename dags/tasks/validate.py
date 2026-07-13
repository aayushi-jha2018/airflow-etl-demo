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
    validator = context.sources.pandas_default.read_dataframe(df)

    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_be_in_set("region", EXPECTED_REGIONS)
    validator.expect_column_values_to_be_between("quantity", min_value=1)
    validator.expect_column_values_to_be_between("total_amount", min_value=0)

    result = validator.validate()

    if not result["success"]:
        failed_columns = [
            r["expectation_config"]["kwargs"].get("column", "?")
            for r in result["results"]
            if not r["success"]
        ]
        raise DataQualityError(f"Data quality checks failed for columns: {failed_columns}")

    return True
