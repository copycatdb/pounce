#!/usr/bin/env python3
"""Load a CSV file into SQL Server using pounce."""

import pounce

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

# One-liner: CSV → SQL Server via Arrow
rows = pounce.ingest_csv(
    "sales_data.csv",
    "sales",
    conn_str=CONN_STR,
    if_exists="replace",
)
print(f"Loaded {rows} rows from CSV into [sales]")
