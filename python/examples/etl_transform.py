#!/usr/bin/env python3
"""Read from SQL Server, transform with Pandas, write back."""

import pounce

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

# Read
df = pounce.read_sql("SELECT * FROM raw_sales", conn_str=CONN_STR)
print(f"Read {len(df)} rows")

# Transform
df["amount_usd"] = df["amount"] * df["exchange_rate"]
df["category"] = df["product_name"].str[:3].str.upper()
summary = df.groupby("category").agg(
    total=("amount_usd", "sum"),
    count=("amount_usd", "count"),
).reset_index()

# Write back
rows = pounce.to_sql(summary, "sales_summary", conn_str=CONN_STR, if_exists="replace")
print(f"Wrote {rows} summary rows")
