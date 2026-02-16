"""Tests for Pandas integration."""

import pytest
import pandas as pd
import numpy as np
from conftest import CONN_STR, drop_table, execute_sql


class TestReadSql:
    def test_basic_read(self, conn, conn_str):
        drop_table(conn, "test_pd_read")
        execute_sql(conn, "CREATE TABLE test_pd_read (id INT, name NVARCHAR(50))")
        execute_sql(conn, "INSERT INTO test_pd_read VALUES (1, N'Alice'), (2, N'Bob')")

        import pounce
        df = pounce.read_sql("SELECT * FROM test_pd_read ORDER BY id", conn=conn)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["id", "name"]
        assert df["id"].tolist() == [1, 2]
        drop_table(conn, "test_pd_read")

    def test_read_with_conn_str(self, conn, conn_str):
        drop_table(conn, "test_pd_cs")
        execute_sql(conn, "CREATE TABLE test_pd_cs (val INT)")
        execute_sql(conn, "INSERT INTO test_pd_cs VALUES (42)")

        import pounce
        df = pounce.read_sql("SELECT * FROM test_pd_cs", conn_str=conn_str)
        assert df["val"].tolist() == [42]
        drop_table(conn, "test_pd_cs")

    def test_read_empty(self, conn):
        import pounce
        df = pounce.read_sql("SELECT 1 AS x WHERE 1=0", conn=conn)
        assert len(df) == 0

    def test_read_with_params(self, conn):
        import pounce
        df = pounce.read_sql("SELECT ? AS val", conn=conn, params=[99])
        assert df["val"].tolist() == [99]

    def test_read_various_dtypes(self, conn):
        import pounce
        df = pounce.read_sql(
            "SELECT CAST(1 AS INT) AS i, CAST(3.14 AS FLOAT) AS f, "
            "N'hello' AS s, CAST(1 AS BIT) AS b",
            conn=conn,
        )
        assert df["i"].tolist() == [1]
        assert abs(df["f"].tolist()[0] - 3.14) < 0.001
        assert df["s"].tolist() == ["hello"]

    def test_read_nulls(self, conn):
        import pounce
        df = pounce.read_sql(
            "SELECT CAST(NULL AS INT) AS ni, CAST(NULL AS NVARCHAR(10)) AS ns",
            conn=conn,
        )
        assert pd.isna(df["ni"].iloc[0])
        assert pd.isna(df["ns"].iloc[0])


class TestToSql:
    def test_basic_write(self, conn, conn_str):
        drop_table(conn, "test_pd_write")
        import pounce
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        rows = pounce.to_sql(df, "test_pd_write", conn=conn, if_exists="fail")
        assert rows == 3

        result = pounce.read_sql("SELECT * FROM test_pd_write ORDER BY id", conn=conn)
        assert len(result) == 3
        drop_table(conn, "test_pd_write")

    def test_replace(self, conn):
        drop_table(conn, "test_pd_repl")
        import pounce
        df1 = pd.DataFrame({"x": [1, 2]})
        pounce.to_sql(df1, "test_pd_repl", conn=conn, if_exists="fail")
        df2 = pd.DataFrame({"x": [10, 20, 30]})
        pounce.to_sql(df2, "test_pd_repl", conn=conn, if_exists="replace")

        result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_pd_repl", conn=conn)
        assert result["c"].tolist() == [3]
        drop_table(conn, "test_pd_repl")

    def test_append(self, conn):
        drop_table(conn, "test_pd_app")
        import pounce
        df = pd.DataFrame({"x": [1]})
        pounce.to_sql(df, "test_pd_app", conn=conn, if_exists="fail")
        pounce.to_sql(df, "test_pd_app", conn=conn, if_exists="append")

        result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_pd_app", conn=conn)
        assert result["c"].tolist() == [2]
        drop_table(conn, "test_pd_app")

    def test_fail_existing(self, conn):
        drop_table(conn, "test_pd_fail")
        import pounce
        df = pd.DataFrame({"x": [1]})
        pounce.to_sql(df, "test_pd_fail", conn=conn, if_exists="fail")
        with pytest.raises(Exception):
            pounce.to_sql(df, "test_pd_fail", conn=conn, if_exists="fail")
        drop_table(conn, "test_pd_fail")

    def test_chunksize(self, conn):
        drop_table(conn, "test_pd_chunk")
        import pounce
        df = pd.DataFrame({"x": list(range(100))})
        pounce.to_sql(df, "test_pd_chunk", conn=conn, if_exists="fail", chunksize=30)

        result = pounce.read_sql("SELECT COUNT(*) AS c FROM test_pd_chunk", conn=conn)
        assert result["c"].tolist() == [100]
        drop_table(conn, "test_pd_chunk")

    def test_roundtrip_with_nulls(self, conn):
        drop_table(conn, "test_pd_rt")
        import pounce
        df = pd.DataFrame({
            "a": pd.array([1, None, 3], dtype=pd.Int64Dtype()),
            "b": ["x", None, "z"],
        })
        pounce.to_sql(df, "test_pd_rt", conn=conn, if_exists="fail")
        result = pounce.read_sql("SELECT * FROM test_pd_rt ORDER BY a", conn=conn)
        assert len(result) == 3
        drop_table(conn, "test_pd_rt")

    def test_no_conn_raises(self):
        import pounce
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError):
            pounce.to_sql(df, "t")

    def test_bad_if_exists(self, conn):
        import pounce
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError):
            pounce.to_sql(df, "t", conn=conn, if_exists="invalid")
