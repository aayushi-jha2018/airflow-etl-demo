"""Extraction task: reads raw sales data from a local CSV.

This stands in for a call to a source API or operational database in a
production pipeline, keeping the demo runnable without external
dependencies or credentials.
"""
import csv
from pathlib import Path
from typing import Dict, List

RAW_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "raw_sales.csv"


def extract_sales_data(path: Path = RAW_DATA_PATH) -> List[Dict[str, str]]:
    """Read raw sales records from a CSV file and return them as a list of dicts."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)
