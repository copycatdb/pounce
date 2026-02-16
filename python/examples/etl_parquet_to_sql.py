#!/usr/bin/env python3
"""Load a Parquet file into SQL Server using pounce."""

import pounce

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

rows = pounce.ingest_parquet(
    "sales_data.parquet",
    "sales",
    conn_str=CONN_STR,
    if_exists="replace",
)
print(f"Loaded {rows} rows from Parquet into [sales]")
