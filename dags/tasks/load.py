"""Load task: writes transformed records to a local output file.

This stands in for a warehouse load (e.g. Snowflake, Redshift) in a
production pipeline, keeping the demo runnable without credentials.
"""
import csv
from pathlib import Path
from typing import Dict, List

OUTPUT_PATH = Path(__file__).resolve().parents[2] / "data" / "processed_sales.csv"


def load_sales_data(records: List[Dict[str, object]], path: Path = OUTPUT_PATH) -> int:
    """Write transformed records to a CSV file and return the number of rows written."""
    if not records:
        return 0

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)

    return len(records)
