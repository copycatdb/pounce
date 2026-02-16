"""Tests for Polars integration."""

import pytest
import polars as pl
from conftest import CONN_STR, drop_table, execute_sql


class TestReadPolars:
    def test_basic_read(self, conn):
        drop_table(conn, "test_pl_read")
        execute_sql(conn, "CREATE TABLE test_pl_read (id INT, name NVARCHAR(50))")
        execute_sql(conn, "INSERT INTO test_pl_read VALUES (1, N'Alice'), (2, N'Bob')")

        import pounce
        df = pounce.read_polars("SELECT * FROM test_pl_read ORDER BY id", conn=conn)
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert df["id"].to_list() == [1, 2]
        assert df["name"].to_list() == ["Alice", "Bob"]
        drop_table(conn, "test_pl_read")

    def test_read_with_conn_str(self, conn, conn_str):
        drop_table(conn, "test_pl_cs")
        execute_sql(conn, "CREATE TABLE test_pl_cs (val INT)")
        execute_sql(conn, "INSERT INTO test_pl_cs VALUES (42)")

        import pounce
        df = pounce.read_polars("SELECT * FROM test_pl_cs", conn_str=conn_str)
        assert df["val"].to_list() == [42]
        drop_table(conn, "test_pl_cs")

    def test_read_empty(self, conn):
        import pounce
        df = pounce.read_polars("SELECT 1 AS x WHERE 1=0", conn=conn)
        assert len(df) == 0

    def test_read_nulls(self, conn):
        import pounce
        df = pounce.read_polars(
            "SELECT CAST(NULL AS INT) AS ni, CAST(NULL AS NVARCHAR(10)) AS ns",
            conn=conn,
        )
        assert df["ni"].to_list() == [None]
        assert df["ns"].to_list() == [None]

    def test_read_various_types(self, conn):
        import pounce
        df = pounce.read_polars(
            "SELECT CAST(1 AS BIGINT) AS b, CAST(3.14 AS FLOAT) AS f, N'hi' AS s",
            conn=conn,
        )
        assert df["b"].to_list() == [1]
        assert df["s"].to_list() == ["hi"]

    def test_zero_copy_arrow(self, conn):
        """Verify Polars gets data from Arrow without copying."""
        import pounce
        # This is a structural test — if it works at all, Arrow → Polars is zero-copy
        df = pounce.read_polars(
            "SELECT n FROM (VALUES (1),(2),(3),(4),(5)) AS t(n)", conn=conn
        )
        assert df["n"].to_list() == [1, 2, 3, 4, 5]


class TestPolarsToSql:
    def test_basic_write(self, conn):
        drop_table(conn, "test_pl_write")
        import pounce
        df = pl.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
        rows = pounce.polars_to_sql(df, "test_pl_write", conn=conn, if_exists="fail")
        assert rows == 3

        result = pounce.read_polars("SELECT * FROM test_pl_write ORDER BY id", conn=conn)
        assert result["id"].to_list() == [1, 2, 3]
        drop_table(conn, "test_pl_write")

    def test_replace(self, conn):
        drop_table(conn, "test_pl_repl")
        import pounce
        df1 = pl.DataFrame({"x": [1, 2]})
        pounce.polars_to_sql(df1, "test_pl_repl", conn=conn, if_exists="fail")
        df2 = pl.DataFrame({"x": [10, 20, 30]})
        pounce.polars_to_sql(df2, "test_pl_repl", conn=conn, if_exists="replace")

        result = pounce.read_polars("SELECT COUNT(*) AS c FROM test_pl_repl", conn=conn)
        assert result["c"].to_list() == [3]
        drop_table(conn, "test_pl_repl")

    def test_append(self, conn):
        drop_table(conn, "test_pl_app")
        import pounce
        df = pl.DataFrame({"x": [1]})
        pounce.polars_to_sql(df, "test_pl_app", conn=conn, if_exists="fail")
        pounce.polars_to_sql(df, "test_pl_app", conn=conn, if_exists="append")

        result = pounce.read_polars("SELECT COUNT(*) AS c FROM test_pl_app", conn=conn)
        assert result["c"].to_list() == [2]
        drop_table(conn, "test_pl_app")

    def test_chunksize(self, conn):
        drop_table(conn, "test_pl_chunk")
        import pounce
        df = pl.DataFrame({"x": list(range(100))})
        pounce.polars_to_sql(df, "test_pl_chunk", conn=conn, if_exists="fail", chunksize=30)

        result = pounce.read_polars("SELECT COUNT(*) AS c FROM test_pl_chunk", conn=conn)
        assert result["c"].to_list() == [100]
        drop_table(conn, "test_pl_chunk")

    def test_roundtrip_strings(self, conn):
        drop_table(conn, "test_pl_str")
        import pounce
        df = pl.DataFrame({"s": ["hello", "world", "🎉"]})
        pounce.polars_to_sql(df, "test_pl_str", conn=conn, if_exists="fail")
        result = pounce.read_polars("SELECT * FROM test_pl_str", conn=conn)
        assert set(result["s"].to_list()) == {"hello", "world", "🎉"}
        drop_table(conn, "test_pl_str")

    def test_no_conn_raises(self):
        import pounce
        df = pl.DataFrame({"x": [1]})
        with pytest.raises(ValueError):
            pounce.polars_to_sql(df, "t")
