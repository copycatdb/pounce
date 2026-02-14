# 🐾 pounce

**Arrow-native SQL Server driver for Python. Zero-copy. Wickedly fast.**

Pounce speaks TDS natively via [tabby](https://github.com/copycatdb/tabby) and returns Apache Arrow record batches through zero-copy FFI. No ODBC. No driver manager. Just raw speed.

## Quick Start

```python
from adbc_driver_mssql import dbapi

conn = dbapi.connect("Server=localhost,1433;UID=sa;PWD=secret;TrustServerCertificate=yes")
cur = conn.cursor()
cur.execute("SELECT * FROM sales_data")

# Zero-copy Arrow table
table = cur.fetch_arrow_table()    

# Or classic DB-API rows
rows = cur.fetchall()
```

## Why Arrow?

Traditional drivers deserialize SQL Server's wire format into Python objects — one `tuple` per row, one Python `float`/`str`/`datetime` per cell. For 100K rows × 9 columns, that's ~900K Python objects on the heap.

Pounce skips all of that. It decodes TDS directly into columnar Arrow arrays in Rust, then hands them to Python via zero-copy FFI. The Python side gets a pointer, not a million objects.

This matters when your data has somewhere to go:

## Benchmarks

*SQL Server 2022, localhost, 100K rows × 9 columns, 10K rows × 20 columns*

### SQL → Polars (zero-copy)

```python
import polars as pl

cur.execute("SELECT * FROM big_table")
df = pl.from_arrow(cur.fetch_arrow_table())
```

| Scenario | Time |
|---|---|
| 100K rows × 9 cols → Polars | **597ms** |
| 10K rows × 20 cols → Polars | **72ms** |

Arrow → Polars is a pointer handoff. No conversion, no copying.

### SQL → Parquet

```python
import pyarrow.parquet as pq

cur.execute("SELECT * FROM big_table")
pq.write_table(cur.fetch_arrow_table(), "output.parquet", compression="zstd")
```

| Scenario | Time | File Size |
|---|---|---|
| 100K rows → Parquet (snappy) | **630ms** | 3.95 MB |
| 100K rows → Parquet (zstd) | **645ms** | 2.24 MB |

SQL Server → Parquet in under a second. Great for ETL and data lake ingestion.

### SQL → DuckDB (zero-copy query)

```python
import duckdb

cur.execute("SELECT * FROM big_table")
arrow_table = cur.fetch_arrow_table()

# DuckDB queries the Arrow table directly — no import step
result = duckdb.sql("""
    SELECT category, COUNT(*), AVG(value)
    FROM arrow_table
    GROUP BY category
""").fetchall()
```

| Scenario | Time |
|---|---|
| 100K rows → DuckDB GROUP BY | **598ms** |
| Two Arrow tables → DuckDB JOIN | **230ms** |

DuckDB reads Arrow tables with zero copying. SQL Server → analytical query in one pipeline.

### SQL → Pandas

```python
cur.execute("SELECT * FROM big_table")
df = cur.fetch_arrow_table().to_pandas()
```

| Scenario | Time |
|---|---|
| 100K rows × 9 cols → Pandas | **646ms** |
| 10K rows × 20 cols → Pandas | **85ms** |

### Arrow Compute (skip DataFrames entirely)

```python
import pyarrow.compute as pc

cur.execute("SELECT * FROM big_table")
table = cur.fetch_arrow_table()

mask = pc.equal(table.column("category"), "electronics")
filtered = table.filter(mask)
avg = pc.mean(filtered.column("value")).as_py()
```

| Scenario | Time |
|---|---|
| 100K rows → filter + aggregate | **587ms** |

When you don't need a DataFrame at all — just filter, aggregate, done.

### Throughput

| Metric | Value |
|---|---|
| Sustained Arrow fetch | **170,000 rows/sec** |
| Multi-query concat (5 queries) | **307ms** for 50K rows |
| SQL → CSV export (100K rows) | **636ms** (13 MB) |

## Architecture

```
┌────────────────────┐
│   Python (PyArrow)  │
├────────────────────┤
│   pounce (PyO3)     │  ← Arrow FFI, zero-copy
├────────────────────┤
│   tabby             │  ← TDS 7.4+ protocol (Rust)
├────────────────────┤
│   TCP / TLS         │
└────────────────────┘
        ↕
   SQL Server
```

No ODBC driver manager. No C++ bindings. Just Rust talking TDS and handing Arrow batches to Python.

## Features

- **Arrow-native** — results as Arrow record batches, not rows-of-tuples
- **Zero-copy FFI** — Arrow arrays cross Rust → Python without copying
- **Full type support** — all SQL Server types mapped to Arrow equivalents
- **DB-API 2.0** — `cursor.fetchall()` works too if you need it
- **Transaction management** — autocommit, begin/commit/rollback
- **Bulk ingest** — load PyArrow tables directly into SQL Server
- **TLS encryption** — rustls, no OpenSSL dependency

## Installation

```bash
pip install adbc-driver-mssql
```

## Part of CopyCat 🐱

| Crate | Role |
|-------|------|
| [tabby](https://github.com/copycatdb/tabby) | TDS 7.4+ protocol engine |
| **pounce** | Arrow-native Python driver |
| [prowl](https://github.com/copycatdb/prowl) | MCP server for AI agents |
| [hiss](https://github.com/copycatdb/hiss) | Python DB-API driver |
| [whiskers](https://github.com/copycatdb/whiskers) | ODBC driver |
| [kibble](https://github.com/copycatdb/kibble) | Node.js driver |
| [claw](https://github.com/copycatdb/claw) | Rust client |
| [nuzzle](https://github.com/copycatdb/nuzzle) | .NET driver |

## License

MIT
