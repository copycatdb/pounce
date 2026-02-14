# adbc-mssql

Arrow-native database driver for Microsoft SQL Server. Zero-copy data transfer from TDS wire protocol to Apache Arrow columnar format.

## Why

Existing Python SQL Server drivers (pyodbc, pymssql) transfer data row-by-row, creating millions of Python objects that are immediately thrown away when converting to DataFrames. This driver skips that entirely:

```
SQL Server → TDS protocol → Rust → Arrow arrays → PyArrow (zero-copy FFI)
```

No intermediate Python objects. TDS rows are decoded directly into Arrow columnar buffers in Rust, then transferred to PyArrow via the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) with zero copy.

**1.6x faster** than row-based approaches at 100k rows, with the gap widening for larger datasets.

## Install

```bash
pip install adbc_driver_mssql
```

Or build from source:
```bash
pip install maturin
git clone https://github.com/saurabh500/adbc-mssql.git
cd adbc-mssql
maturin develop --release
```

## Quick Start

```python
import adbc_driver_mssql.dbapi as mssql

conn = mssql.connect("Server=localhost,1433;UID=sa;PWD=yourpassword;TrustServerCertificate=yes")
cur = conn.cursor()

# Arrow-native fetch — the fast path
cur.execute("SELECT * FROM large_table")
table = cur.fetch_arrow_table()
df = table.to_pandas()  # zero-copy to Pandas

# Streaming large results batch-by-batch
cur.execute("SELECT * FROM huge_table")
for batch in cur.fetch_record_batch(batch_size=65536):
    process(batch)

# Standard DB-API 2.0
cur.execute("SELECT id, name FROM users WHERE id = ?", [42])
rows = cur.fetchall()

# Bulk ingestion from Arrow
import pyarrow as pa
table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
cur.adbc_ingest("target_table", table)

# Transactions
conn.commit()
conn.rollback()
conn.close()
```

## API

Follows the [adbc_driver_postgresql](https://arrow.apache.org/adbc/current/python/api/adbc_driver_postgresql.html) API pattern. DB-API 2.0 compatible.

| Method | Description |
|---|---|
| `connect(conn_str)` | Open a connection |
| `cursor.execute(sql, params?)` | Execute a query |
| `cursor.fetch_arrow_table()` | Fetch all results as a PyArrow Table |
| `cursor.fetch_record_batch(batch_size?)` | Stream results as RecordBatches |
| `cursor.fetchone()` | Fetch one row (DB-API) |
| `cursor.fetchall()` | Fetch all rows as tuples (DB-API) |
| `cursor.fetchmany(size)` | Fetch N rows (DB-API) |
| `cursor.adbc_ingest(table_name, arrow_table)` | Bulk insert from Arrow |
| `conn.commit()` / `conn.rollback()` | Transaction control |
| `conn.autocommit` | Get/set autocommit mode |

## Type Mapping

| SQL Server | Arrow |
|---|---|
| `TINYINT` | `UInt8` |
| `SMALLINT` | `Int16` |
| `INT` | `Int32` |
| `BIGINT` | `Int64` |
| `REAL` | `Float32` |
| `FLOAT` | `Float64` |
| `BIT` | `Boolean` |
| `VARCHAR` / `NVARCHAR` / `TEXT` | `Utf8` |
| `BINARY` / `VARBINARY` / `IMAGE` | `Binary` |
| `DECIMAL` / `NUMERIC` | `Decimal128` |
| `MONEY` / `SMALLMONEY` | `Decimal128(19,4)` |
| `DATE` | `Date32` |
| `TIME` | `Time64(Microsecond)` |
| `DATETIME` / `DATETIME2` | `Timestamp(Microsecond)` |
| `DATETIMEOFFSET` | `Timestamp(Microsecond, tz)` |
| `UNIQUEIDENTIFIER` | `Utf8` |

## Architecture

Under the hood lives **tabby** 🐱 — our embedded TDS 7.4+ protocol implementation in pure Rust. Tabby speaks fluent SQL Server, handles all the gnarly wire protocol details (authentication, encryption, type encoding, token streams), and does it without a single C dependency. No ODBC. No FreeTDS. Just a cat that really understands packets.

Tabby's job is simple: catch the data coming off the wire and drop it straight into Arrow columnar buffers. Like a cat bringing you mice, except the mice are RecordBatches and they're actually useful.

```
┌─────────────────────────────────┐
│  Python (DB-API 2.0 / PyArrow)  │
├─────────────────────────────────┤
│  PyO3 native extension (Rust)   │
├─────────────────────────────────┤
│  tabby 🐱 (TDS 7.4+ protocol)  │  ← pure Rust, no ODBC
├─────────────────────────────────┤
│  arrow-rs (Array Builders)      │  ← rows → columns in Rust
├─────────────────────────────────┤
│  Arrow C Data Interface (FFI)   │  ← zero-copy to PyArrow
└─────────────────────────────────┘
```

Key design decisions:
- **No ODBC dependency** — tabby handles TDS natively in pure Rust
- **No Python in the data path** — row decoding + columnar conversion happens entirely in Rust
- **Arrow FFI** — RecordBatches cross the Rust→Python boundary without serialization
- **Configurable batch size** — accumulate N rows per Arrow batch (default 65536)

## Building

Requires:
- Rust 1.70+
- Python 3.9+
- maturin

```bash
# Development build
maturin develop

# Release build
maturin develop --release

# Build wheel
maturin build --release
```

## Testing

```bash
# Requires SQL Server on localhost:1433
pip install pytest pyarrow
pytest tests/
```

## Attribution

This project's TDS protocol implementation is inspired by and derived from [tiberius](https://github.com/prisma/tiberius), a TDS driver for Rust.

## License

MIT
