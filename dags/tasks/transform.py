"""Transformation task: cleans and enriches raw sales records."""
from typing import Dict, List


def transform_sales_data(records: List[Dict[str, str]]) -> List[Dict[str, object]]:
    """Cast types, standardize region names, and compute a total per order.

    Args:
        records: Raw rows as read from the source CSV (all string values).

    Returns:
        Cleaned records with proper types and a computed ``total_amount``.
    """
    transformed: List[Dict[str, object]] = []
    for row in records:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        transformed.append(
            {
                "order_id": row["order_id"],
                "region": row["region"].strip().title(),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
            }
        )
    return transformed
