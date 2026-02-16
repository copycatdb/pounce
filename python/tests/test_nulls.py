"""Tests for NULL handling across all interfaces."""

import pytest
import pandas as pd
from conftest import CONN_STR, drop_table, execute_sql


NULL_COLUMNS = """
    CAST(NULL AS INT) AS ni,
    CAST(NULL AS BIGINT) AS nb,
    CAST(NULL AS SMALLINT) AS ns,
    CAST(NULL AS TINYINT) AS nt,
    CAST(NULL AS FLOAT) AS nf,
    CAST(NULL AS REAL) AS nr,
    CAST(NULL AS BIT) AS nbit,
    CAST(NULL AS VARCHAR(10)) AS nvc,
    CAST(NULL AS NVARCHAR(10)) AS nnvc,
    CAST(NULL AS DATE) AS nd,
    CAST(NULL AS DATETIME2) AS ndt,
    CAST(NULL AS DECIMAL(10,2)) AS ndec,
    CAST(NULL AS UNIQUEIDENTIFIER) AS nuid,
    CAST(NULL AS VARBINARY(10)) AS nvb
"""


class TestNullsDbapi:
    def test_all_nulls(self, conn):
        cur = conn.cursor()
        cur.execute(f"SELECT {NULL_COLUMNS}")
        row = cur.fetchone()
        assert all(v is None for v in row)

    def test_nulls_in_arrow(self, conn):
        cur = conn.cursor()
        cur.execute(f"SELECT {NULL_COLUMNS}")
        table = cur.fetch_arrow_table()
        assert table.num_rows == 1
        for i in range(table.num_columns):
            assert table.column(i).null_count == 1

    def test_mixed_nulls(self, conn):
        """Some rows null, some not."""
        cur = conn.cursor()
        cur.execute("""
            SELECT v FROM (VALUES
                (CAST(1 AS INT)),
                (CAST(NULL AS INT)),
                (CAST(3 AS INT))
            ) AS t(v)
        """)
        rows = cur.fetchall()
        assert rows[0] == (1,)
        assert rows[1] == (None,)
        assert rows[2] == (3,)


class TestNullsPandas:
    def test_all_nulls(self, conn):
        import pounce
        df = pounce.read_sql(f"SELECT {NULL_COLUMNS}", conn=conn)
        assert len(df) == 1
        assert df.isnull().all(axis=1).iloc[0]

    def test_null_int_column(self, conn):
        import pounce
        df = pounce.read_sql("""
            SELECT v FROM (VALUES
                (CAST(1 AS INT)), (CAST(NULL AS INT)), (CAST(3 AS INT))
            ) AS t(v)
        """, conn=conn)
        assert len(df) == 3
        assert pd.isna(df["v"].iloc[1])

    def test_write_nulls(self, conn):
        drop_table(conn, "test_null_pd_w")
        import pounce
        df = pd.DataFrame({
            "a": pd.array([1, None, 3], dtype=pd.Int64Dtype()),
            "b": [None, "hello", None],
            "c": [1.0, None, 3.0],
        })
        pounce.to_sql(df, "test_null_pd_w", conn=conn, if_exists="fail")
        result = pounce.read_sql("SELECT * FROM test_null_pd_w ORDER BY c", conn=conn)
        assert len(result) == 3
        drop_table(conn, "test_null_pd_w")


class TestNullsPolars:
    def test_all_nulls(self, conn):
        import pounce
        df = pounce.read_polars(f"SELECT {NULL_COLUMNS}", conn=conn)
        assert len(df) == 1
        for col in df.columns:
            assert df[col].null_count() == 1

    def test_mixed_nulls(self, conn):
        import pounce
        df = pounce.read_polars("""
            SELECT v FROM (VALUES
                (CAST(1 AS INT)), (CAST(NULL AS INT)), (CAST(3 AS INT))
            ) AS t(v)
        """, conn=conn)
        assert df["v"].to_list() == [1, None, 3]
