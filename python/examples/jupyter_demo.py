#!/usr/bin/env python3
"""Jupyter-style demo of pounce capabilities.

Run this as a script or paste cells into a Jupyter notebook.
"""

# %% [markdown]
# # 🐾 Pounce — Arrow-native SQL Server driver
# Zero-copy. Wickedly fast. Works with Pandas, Polars, and raw Arrow.

# %% Cell 1: Connect and query
import pounce

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

conn = pounce.connect(CONN_STR)
conn.autocommit = True

# %% Cell 2: Create sample data
cur = conn.cursor()
cur.execute("IF OBJECT_ID('demo_sales', 'U') IS NOT NULL DROP TABLE demo_sales")
cur.execute("""
    CREATE TABLE demo_sales (
        id INT, product NVARCHAR(50), amount DECIMAL(10,2), sold_at DATETIME2
    )
""")
cur.execute("""
    INSERT INTO demo_sales VALUES
    (1, N'Widget', 29.99, '2024-01-15 10:30:00'),
    (2, N'Gadget', 149.50, '2024-01-16 14:00:00'),
    (3, N'Widget', 29.99, '2024-02-01 09:15:00'),
    (4, N'Doohickey', 9.99, '2024-02-14 16:45:00'),
    (5, N'Gadget', 149.50, '2024-03-01 11:00:00')
""")
print("✅ Sample data created")

# %% Cell 3: Read into Pandas
df = pounce.read_sql("SELECT * FROM demo_sales ORDER BY id", conn=conn)
print(df)
print(f"\nShape: {df.shape}")
print(f"Dtypes:\n{df.dtypes}")

# %% Cell 4: Read into Polars (zero-copy!)
df_pl = pounce.read_polars("SELECT * FROM demo_sales ORDER BY id", conn=conn)
print(df_pl)

# %% Cell 5: Arrow table directly
cur.execute("SELECT * FROM demo_sales")
arrow_table = cur.fetch_arrow_table()
print(f"Arrow table: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
print(f"Schema: {arrow_table.schema}")

# %% Cell 6: Export to Parquet
import tempfile, os
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
    path = f.name
rows = pounce.export_parquet("SELECT * FROM demo_sales", path, conn=conn, compression="zstd")
size = os.path.getsize(path)
print(f"Exported {rows} rows to Parquet ({size} bytes)")
os.unlink(path)

# %% Cell 7: Pandas transform and write back
summary = df.groupby("product").agg(
    total_amount=("amount", "sum"),
    num_sales=("id", "count"),
).reset_index()
print(summary)
pounce.to_sql(summary, "demo_summary", conn=conn, if_exists="replace")
print("✅ Summary written to SQL Server")

# %% Cell 8: Verify
result = pounce.read_sql("SELECT * FROM demo_summary ORDER BY total_amount DESC", conn=conn)
print(result)

# %% Cleanup
cur.execute("DROP TABLE IF EXISTS demo_sales")
cur.execute("DROP TABLE IF EXISTS demo_summary")
conn.close()
print("✅ Done!")
