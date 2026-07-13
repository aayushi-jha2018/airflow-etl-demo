"""Unit tests for the data-quality validation task.

These specifically check that validate_sales_data raises DataQualityError on
bad data, not just that it passes on good data -- the point of this task is
to catch problems before they reach load_sales_data().
"""

import pytest

from tasks.validate import DataQualityError, validate_sales_data

GOOD_RECORDS = [
    {"order_id": "5001", "region": "North", "quantity": 3, "unit_price": 19.99, "total_amount": 59.97},
    {"order_id": "5002", "region": "South", "quantity": 5, "unit_price": 9.50, "total_amount": 47.50},
]


def test_valid_records_pass():
    assert validate_sales_data(GOOD_RECORDS) is True


def test_empty_records_raise():
    with pytest.raises(DataQualityError):
        validate_sales_data([])


def test_duplicate_order_id_is_caught():
    bad = GOOD_RECORDS + [dict(GOOD_RECORDS[0])]  # order_id 5001 repeated
    with pytest.raises(DataQualityError):
        validate_sales_data(bad)


def test_invalid_region_is_caught():
    bad = [dict(GOOD_RECORDS[0], order_id="5003", region="Narnia")]
    with pytest.raises(DataQualityError):
        validate_sales_data(bad)


def test_negative_quantity_is_caught():
    bad = [dict(GOOD_RECORDS[0], order_id="5004", quantity=-1)]
    with pytest.raises(DataQualityError):
        validate_sales_data(bad)
