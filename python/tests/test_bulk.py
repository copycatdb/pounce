"""Tests for bulk load performance and schema creation."""

import pytest
import pyarrow as pa
import pandas as pd
from conftest import CONN_STR, drop_table, execute_sql


class TestBulkLoad:
    def test_10k_rows(self, conn):
        """Bulk load 10K rows via Arrow ingest."""
        drop_table(conn, "test_bulk_10k")
        n = 10000
        table = pa.table({
            "id": list(range(n)),
            "value": [float(i) * 1.1 for i in range(n)],
            "label": [f"row_{i}" for i in range(n)],
        })
        cur = conn.cursor()
        cur.adbc_ingest("test_bulk_10k", table, mode="create")
        assert cur.rowcount == n

        cur.execute("SELECT COUNT(*) AS c FROM test_bulk_10k")
        assert cur.fetchone()[0] == n
        drop_table(conn, "test_bulk_10k")

    def test_50k_rows(self, conn):
        """Bulk load 50K rows."""
        drop_table(conn, "test_bulk_50k")
        n = 50000
        table = pa.table({
            "id": list(range(n)),
            "val": [i * 0.5 for i in range(n)],
        })
        cur = conn.cursor()
        cur.adbc_ingest("test_bulk_50k", table, mode="create")
        assert cur.rowcount == n

        cur.execute("SELECT COUNT(*) AS c FROM test_bulk_50k")
        assert cur.fetchone()[0] == n
        drop_table(conn, "test_bulk_50k")

    def test_bulk_pandas(self, conn):
        """Bulk load via Pandas to_sql."""
        drop_table(conn, "test_bulk_pd")
        import pounce
        n = 10000
        df = pd.DataFrame({
            "x": list(range(n)),
            "y": [f"val_{i}" for i in range(n)],
        })
        rows = pounce.to_sql(df, "test_bulk_pd", conn=conn, if_exists="fail")
        assert rows == n
        drop_table(conn, "test_bulk_pd")

    def test_bulk_polars(self, conn):
        """Bulk load via Polars polars_to_sql."""
        drop_table(conn, "test_bulk_pl")
        import polars as pl
        import pounce
        n = 10000
        df = pl.DataFrame({
            "x": list(range(n)),
            "y": [f"val_{i}" for i in range(n)],
        })
        rows = pounce.polars_to_sql(df, "test_bulk_pl", conn=conn, if_exists="fail")
        assert rows == n
        drop_table(conn, "test_bulk_pl")


class TestSchemaCreation:
    def test_create_mode(self, conn):
        """'create' mode should create the table."""
        drop_table(conn, "test_schema_create")
        table = pa.table({"a": [1], "b": ["hello"]})
        cur = conn.cursor()
        cur.adbc_ingest("test_schema_create", table, mode="create")
        # Verify table exists
        cur.execute("SELECT COUNT(*) FROM test_schema_create")
        assert cur.fetchone()[0] == 1
        drop_table(conn, "test_schema_create")

    def test_replace_mode(self, conn):
        """'replace' mode should drop and recreate."""
        drop_table(conn, "test_schema_repl")
        table1 = pa.table({"a": [1, 2]})
        cur = conn.cursor()
        cur.adbc_ingest("test_schema_repl", table1, mode="create")
        table2 = pa.table({"a": [10, 20, 30]})
        cur.adbc_ingest("test_schema_repl", table2, mode="replace")
        cur.execute("SELECT COUNT(*) FROM test_schema_repl")
        assert cur.fetchone()[0] == 3
        drop_table(conn, "test_schema_repl")

    def test_append_mode(self, conn):
        drop_table(conn, "test_schema_app")
        table = pa.table({"a": [1]})
        cur = conn.cursor()
        cur.adbc_ingest("test_schema_app", table, mode="create")
        cur.adbc_ingest("test_schema_app", table, mode="append")
        cur.execute("SELECT COUNT(*) FROM test_schema_app")
        assert cur.fetchone()[0] == 2
        drop_table(conn, "test_schema_app")

    def test_wide_table(self, conn):
        """Table with many columns."""
        drop_table(conn, "test_wide")
        data = {f"col_{i}": [i] for i in range(50)}
        table = pa.table(data)
        cur = conn.cursor()
        cur.adbc_ingest("test_wide", table, mode="create")
        cur.execute("SELECT COUNT(*) FROM test_wide")
        assert cur.fetchone()[0] == 1
        drop_table(conn, "test_wide")
