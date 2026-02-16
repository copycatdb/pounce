"""Tests for CSV/Parquet ingest and export."""

import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from conftest import CONN_STR, drop_table, execute_sql


class TestIngestCsv:
    def test_basic_csv_ingest(self, conn):
        drop_table(conn, "test_csv_in")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
            path = f.name
        try:
            import pounce
            rows = pounce.ingest_csv(path, "test_csv_in", conn=conn, if_exists="fail")
            assert rows == 3

            result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_csv_in", conn=conn)
            assert result["c"].tolist() == [3]
        finally:
            os.unlink(path)
            drop_table(conn, "test_csv_in")

    def test_csv_replace(self, conn):
        drop_table(conn, "test_csv_repl")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("x\n1\n2\n")
            path = f.name
        try:
            import pounce
            pounce.ingest_csv(path, "test_csv_repl", conn=conn, if_exists="fail")
            pounce.ingest_csv(path, "test_csv_repl", conn=conn, if_exists="replace")
            result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_csv_repl", conn=conn)
            assert result["c"].tolist() == [2]
        finally:
            os.unlink(path)
            drop_table(conn, "test_csv_repl")


class TestIngestParquet:
    def test_basic_parquet_ingest(self, conn):
        drop_table(conn, "test_pq_in")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        table = pa.table({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
        pq.write_table(table, path)
        try:
            import pounce
            rows = pounce.ingest_parquet(path, "test_pq_in", conn=conn, if_exists="fail")
            assert rows == 3
        finally:
            os.unlink(path)
            drop_table(conn, "test_pq_in")

    def test_parquet_with_columns(self, conn):
        drop_table(conn, "test_pq_cols")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        table = pa.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        pq.write_table(table, path)
        try:
            import pounce
            rows = pounce.ingest_parquet(path, "test_pq_cols", conn=conn, if_exists="fail", columns=["a", "c"])
            assert rows == 2
            result = pounce.read_sql("SELECT * FROM test_pq_cols", conn=conn)
            assert set(result.columns) == {"a", "c"}
        finally:
            os.unlink(path)
            drop_table(conn, "test_pq_cols")


class TestExportParquet:
    def test_basic_export(self, conn):
        drop_table(conn, "test_exp_pq")
        execute_sql(conn, "CREATE TABLE test_exp_pq (id INT, name NVARCHAR(50))")
        execute_sql(conn, "INSERT INTO test_exp_pq VALUES (1, N'Alice'), (2, N'Bob')")

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            import pounce
            rows = pounce.export_parquet("SELECT * FROM test_exp_pq", path, conn=conn)
            assert rows == 2
            table = pq.read_table(path)
            assert table.num_rows == 2
        finally:
            os.unlink(path)
            drop_table(conn, "test_exp_pq")

    def test_export_compression(self, conn):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            import pounce
            pounce.export_parquet(
                "SELECT n FROM (VALUES (1),(2),(3)) AS t(n)",
                path, conn=conn, compression="zstd"
            )
            table = pq.read_table(path)
            assert table.num_rows == 3
        finally:
            os.unlink(path)


class TestExportCsv:
    def test_basic_export(self, conn):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            import pounce
            rows = pounce.export_csv(
                "SELECT n FROM (VALUES (1),(2),(3)) AS t(n)",
                path, conn=conn,
            )
            assert rows == 3
            with open(path) as f:
                lines = f.readlines()
            assert len(lines) == 4  # header + 3 data rows
        finally:
            os.unlink(path)


class TestRoundtrip:
    def test_csv_roundtrip(self, conn):
        """Write to SQL, export CSV, re-import, verify."""
        drop_table(conn, "test_rt_src")
        drop_table(conn, "test_rt_dst")
        execute_sql(conn, "CREATE TABLE test_rt_src (id INT, val FLOAT)")
        execute_sql(conn, "INSERT INTO test_rt_src VALUES (1, 1.1), (2, 2.2), (3, 3.3)")

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            import pounce
            pounce.export_csv("SELECT * FROM test_rt_src ORDER BY id", path, conn=conn)
            pounce.ingest_csv(path, "test_rt_dst", conn=conn, if_exists="fail")
            result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_rt_dst", conn=conn)
            assert result["c"].tolist() == [3]
        finally:
            os.unlink(path)
            drop_table(conn, "test_rt_src")
            drop_table(conn, "test_rt_dst")

    def test_parquet_roundtrip(self, conn):
        """Write to SQL, export Parquet, re-import, verify."""
        drop_table(conn, "test_rt_pq_src")
        drop_table(conn, "test_rt_pq_dst")
        execute_sql(conn, "CREATE TABLE test_rt_pq_src (id INT, name NVARCHAR(50))")
        execute_sql(conn, "INSERT INTO test_rt_pq_src VALUES (1, N'hello'), (2, N'world')")

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            import pounce
            pounce.export_parquet("SELECT * FROM test_rt_pq_src ORDER BY id", path, conn=conn)
            pounce.ingest_parquet(path, "test_rt_pq_dst", conn=conn, if_exists="fail")
            result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_rt_pq_dst", conn=conn)
            assert result["c"].tolist() == [2]
        finally:
            os.unlink(path)
            drop_table(conn, "test_rt_pq_src")
            drop_table(conn, "test_rt_pq_dst")
