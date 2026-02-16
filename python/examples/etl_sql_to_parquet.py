#!/usr/bin/env python3
"""Export SQL Server query results to Parquet."""

import pounce

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

rows = pounce.export_parquet(
    "SELECT * FROM sales WHERE amount > 100",
    "high_value_sales.parquet",
    conn_str=CONN_STR,
    compression="zstd",
)
print(f"Exported {rows} rows to high_value_sales.parquet")
