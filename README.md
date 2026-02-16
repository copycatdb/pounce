# 🐾 pounce

**Arrow-native SQL Server driver for Python. Zero-copy. Wickedly fast.**

Pounce speaks TDS natively via [tabby](https://github.com/copycatdb/tabby) and returns Apache Arrow record batches through zero-copy FFI. No ODBC. No driver manager. Just raw speed.

## Why pounce?

| Feature | pounce | pyodbc + pandas |
|---------|--------|----------------|
| Wire protocol | Native TDS (Rust) | ODBC driver manager |
| Data format | Arrow columnar (zero-copy) | Row-by-row Python objects |
| 100K rows → Pandas | **~650ms** | ~3-5s |
| 100K rows → Polars | **~600ms** (zero-copy) | N/A |
| Memory overhead | Minimal (Arrow buffers) | ~900K Python objects |
| SQL → Parquet | **~630ms** direct | Multiple conversions |
| Dependencies | None (self-contained Rust) | ODBC driver + manager |

Arrow is the key. Traditional drivers deserialize SQL Server's wire format into Python objects — one `tuple` per row, one `float`/`str`/`datetime` per cell. Pounce skips all of that: it decodes TDS directly into columnar Arrow arrays in Rust, then hands them to Python via zero-copy FFI.

## Installation

```bash
pip install copycatdb-pounce
```

Optional dependencies for integrations:
```bash
pip install pandas polars pyarrow tqdm
```

## Quick Start

### DB-API 2.0

```python
import pounce

conn = pounce.connect("Server=localhost,1433;UID=sa;PWD=secret;TrustServerCertificate=yes")
cur = conn.cursor()
cur.execute("SELECT * FROM sales")

# Zero-copy Arrow table
table = cur.fetch_arrow_table()

# Or classic DB-API rows
rows = cur.fetchall()
```

### Pandas Integration

```python
import pounce

# SQL → DataFrame (via Arrow, near-zero-copy)
df = pounce.read_sql("SELECT * FROM sales", conn_str="Server=localhost,1433;UID=sa;PWD=secret")

# DataFrame → SQL Server (via Arrow bulk load)
pounce.to_sql(df, "target_table", conn_str="...", if_exists="replace")

# Supports chunksize for large DataFrames
pounce.to_sql(big_df, "target", conn_str="...", if_exists="append", chunksize=10000)
```

### Polars Integration (True Zero-Copy)

```python
import pounce

# SQL → Polars DataFrame (Arrow → Polars is a pointer handoff)
df = pounce.read_polars("SELECT * FROM sales", conn_str="...")

# Polars → SQL Server
pounce.polars_to_sql(df, "target_table", conn_str="...", if_exists="replace")
```

### CSV & Parquet Ingest/Export

```python
import pounce

# CSV → SQL Server (reads via Arrow, bulk loads)
pounce.ingest_csv("data.csv", "sales", conn_str="...", if_exists="replace")

# Parquet → SQL Server
pounce.ingest_parquet("data.parquet", "sales", conn_str="...")

# SQL Server → Parquet (with compression)
pounce.export_parquet("SELECT * FROM sales", "output.parquet", conn_str="...", compression="zstd")

# SQL Server → CSV
pounce.export_csv("SELECT * FROM sales", "output.csv", conn_str="...")
```

## Benchmarks

*SQL Server 2022, localhost, 100K rows × 9 columns, 10K rows × 20 columns*

### SQL → Polars (zero-copy)

| Scenario | Time |
|---|---|
| 100K rows × 9 cols → Polars | **597ms** |
| 10K rows × 20 cols → Polars | **72ms** |

### SQL → Pandas

| Scenario | Time |
|---|---|
| 100K rows × 9 cols → Pandas | **646ms** |
| 10K rows × 20 cols → Pandas | **85ms** |

### SQL → Parquet

| Scenario | Time | File Size |
|---|---|---|
| 100K rows → Parquet (snappy) | **630ms** | 3.95 MB |
| 100K rows → Parquet (zstd) | **645ms** | 2.24 MB |

### SQL → DuckDB (zero-copy query)

| Scenario | Time |
|---|---|
| 100K rows → DuckDB GROUP BY | **598ms** |
| Two Arrow tables → DuckDB JOIN | **230ms** |

### Throughput

| Metric | Value |
|---|---|
| Sustained Arrow fetch | **170,000 rows/sec** |
| Multi-query concat (5 queries) | **307ms** for 50K rows |
| SQL → CSV export (100K rows) | **636ms** (13 MB) |

## API Reference

### Top-level functions

| Function | Description |
|----------|-------------|
| `pounce.connect(conn_str)` | Open a DB-API 2.0 connection |
| `pounce.read_sql(query, conn=, conn_str=)` | Query → Pandas DataFrame |
| `pounce.to_sql(df, table, conn=, conn_str=, if_exists=)` | Pandas DataFrame → SQL Server |
| `pounce.read_polars(query, conn=, conn_str=)` | Query → Polars DataFrame |
| `pounce.polars_to_sql(df, table, conn=, conn_str=)` | Polars DataFrame → SQL Server |
| `pounce.ingest_csv(path, table, conn=, conn_str=)` | CSV file → SQL Server |
| `pounce.ingest_parquet(path, table, conn=, conn_str=)` | Parquet file → SQL Server |
| `pounce.export_csv(query, path, conn=, conn_str=)` | Query → CSV file |
| `pounce.export_parquet(query, path, conn=, conn_str=)` | Query → Parquet file |

### `to_sql` / `polars_to_sql` options

- `if_exists="fail"` — error if table exists (default)
- `if_exists="replace"` — drop and recreate
- `if_exists="append"` — insert into existing table
- `chunksize=N` — write in batches of N rows
- `dtype={"col": pa.int64()}` — override Arrow type mapping (Pandas only)

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

## Features

- **Arrow-native** — results as Arrow record batches, not rows-of-tuples
- **Zero-copy FFI** — Arrow arrays cross Rust → Python without copying
- **Pandas & Polars** — first-class DataFrame integration
- **CSV/Parquet ingest** — file → SQL Server in one call
- **Full type support** — all SQL Server types mapped to Arrow equivalents
- **DB-API 2.0** — drop-in replacement for existing code
- **Bulk ingest** — load Arrow tables directly into SQL Server
- **Transaction management** — autocommit, begin/commit/rollback
- **TLS encryption** — rustls, no OpenSSL dependency

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
