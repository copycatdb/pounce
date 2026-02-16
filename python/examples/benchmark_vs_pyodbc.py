#!/usr/bin/env python3
"""Benchmark pounce vs pyodbc+pandas for reading and writing data.

Requirements: pip install pyodbc pounce pandas
"""

import time
import pandas as pd

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"
PYODBC_CONN_STR = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"

N_ROWS = 100_000


def setup_test_data():
    """Create a test table with N_ROWS rows."""
    import pounce
    conn = pounce.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS bench_data")
    except Exception:
        pass
    cur.execute("""
        CREATE TABLE bench_data (
            id INT, value FLOAT, category NVARCHAR(50),
            amount DECIMAL(10,2), flag BIT, label VARCHAR(100)
        )
    """)
    # Bulk load test data
    import pyarrow as pa
    table = pa.table({
        "id": list(range(N_ROWS)),
        "value": [float(i) * 1.1 for i in range(N_ROWS)],
        "category": [f"cat_{i % 10}" for i in range(N_ROWS)],
        "amount": [round(i * 0.99, 2) for i in range(N_ROWS)],
        "flag": [i % 2 == 0 for i in range(N_ROWS)],
        "label": [f"label_{i}" for i in range(N_ROWS)],
    })
    cur.adbc_ingest("bench_data", table, mode="replace")
    conn.close()
    print(f"Setup: {N_ROWS:,} rows in bench_data")


def bench_pounce_read():
    import pounce
    start = time.perf_counter()
    df = pounce.read_sql("SELECT * FROM bench_data", conn_str=CONN_STR)
    elapsed = time.perf_counter() - start
    print(f"pounce read_sql: {elapsed:.3f}s ({len(df):,} rows)")
    return elapsed


def bench_pyodbc_read():
    try:
        import pyodbc
    except ImportError:
        print("pyodbc not installed, skipping")
        return None
    start = time.perf_counter()
    conn = pyodbc.connect(PYODBC_CONN_STR)
    df = pd.read_sql("SELECT * FROM bench_data", conn)
    conn.close()
    elapsed = time.perf_counter() - start
    print(f"pyodbc read_sql: {elapsed:.3f}s ({len(df):,} rows)")
    return elapsed


def bench_pounce_write():
    import pounce
    df = pd.DataFrame({
        "x": list(range(N_ROWS)),
        "y": [f"val_{i}" for i in range(N_ROWS)],
    })
    start = time.perf_counter()
    pounce.to_sql(df, "bench_write_pounce", conn_str=CONN_STR, if_exists="replace")
    elapsed = time.perf_counter() - start
    print(f"pounce to_sql:   {elapsed:.3f}s ({N_ROWS:,} rows)")
    return elapsed


if __name__ == "__main__":
    print(f"Benchmarking with {N_ROWS:,} rows\n")
    setup_test_data()
    print()

    t_pounce = bench_pounce_read()
    t_pyodbc = bench_pyodbc_read()
    if t_pyodbc:
        print(f"\nRead speedup: {t_pyodbc / t_pounce:.1f}x faster")

    print()
    bench_pounce_write()
