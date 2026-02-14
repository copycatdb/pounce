"""Tests for pounce"""
import pytest
import pyarrow as pa
import time

CONN_STR = "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes"


@pytest.fixture
def conn():
    import pounce.dbapi as mssql
    c = mssql.connect(CONN_STR)
    c.autocommit = True
    yield c
    c.close()


@pytest.fixture
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()


class TestConnection:
    def test_connect(self):
        import pounce.dbapi as mssql
        conn = mssql.connect(CONN_STR)
        assert conn is not None
        conn.close()

    def test_autocommit(self, conn):
        conn.autocommit = True
        assert conn.autocommit is True
        conn.autocommit = False
        assert conn.autocommit is False


class TestBasicQueries:
    def test_select_one(self, cursor):
        cursor.execute("SELECT 1 AS val")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1

    def test_select_string(self, cursor):
        cursor.execute("SELECT N'hello' AS greeting")
        rows = cursor.fetchall()
        assert rows[0][0] == "hello"

    def test_parameterized_query(self, cursor):
        cursor.execute("SELECT ? AS val", [42])
        rows = cursor.fetchall()
        assert rows[0][0] == 42

    def test_fetchone(self, cursor):
        cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        assert cursor.fetchone()[0] == 1
        assert cursor.fetchone()[0] == 2
        assert cursor.fetchone()[0] == 3
        assert cursor.fetchone() is None

    def test_fetchmany(self, cursor):
        cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        rows = cursor.fetchmany(2)
        assert len(rows) == 2

    def test_description(self, cursor):
        cursor.execute("SELECT 1 AS num, N'abc' AS txt")
        assert cursor.description is not None
        assert cursor.description[0][0] == "num"
        assert cursor.description[1][0] == "txt"


class TestArrowFetch:
    def test_fetch_arrow_table(self, cursor):
        cursor.execute("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
        table = cursor.fetch_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.num_columns == 2
        assert table.column_names == ["a", "b"]

    def test_arrow_types_int(self, cursor):
        cursor.execute("""
            SELECT
                CAST(1 AS TINYINT) AS tiny,
                CAST(2 AS SMALLINT) AS small,
                CAST(3 AS INT) AS normal,
                CAST(4 AS BIGINT) AS big
        """)
        table = cursor.fetch_arrow_table()
        assert table.schema.field("tiny").type == pa.uint8()
        assert table.schema.field("small").type == pa.int16()
        assert table.schema.field("normal").type == pa.int32()
        assert table.schema.field("big").type == pa.int64()

    def test_arrow_types_float(self, cursor):
        cursor.execute("SELECT CAST(1.5 AS REAL) AS r, CAST(2.5 AS FLOAT) AS f")
        table = cursor.fetch_arrow_table()
        assert table.schema.field("r").type == pa.float32()
        assert table.schema.field("f").type == pa.float64()

    def test_arrow_types_string(self, cursor):
        cursor.execute("SELECT CAST('hello' AS NVARCHAR(50)) AS s")
        table = cursor.fetch_arrow_table()
        assert table.schema.field("s").type == pa.utf8()
        assert table.column("s")[0].as_py() == "hello"

    def test_arrow_types_bool(self, cursor):
        cursor.execute("SELECT CAST(1 AS BIT) AS b")
        table = cursor.fetch_arrow_table()
        assert table.schema.field("b").type == pa.bool_()
        assert table.column("b")[0].as_py() is True

    def test_arrow_types_date(self, cursor):
        cursor.execute("SELECT CAST('2024-01-15' AS DATE) AS d")
        table = cursor.fetch_arrow_table()
        assert table.schema.field("d").type == pa.date32()

    def test_arrow_types_decimal(self, cursor):
        cursor.execute("SELECT CAST(123.45 AS DECIMAL(10,2)) AS d")
        table = cursor.fetch_arrow_table()
        assert pa.types.is_decimal(table.schema.field("d").type)

    def test_arrow_types_binary(self, cursor):
        cursor.execute("SELECT CAST(0xDEADBEEF AS VARBINARY(10)) AS b")
        table = cursor.fetch_arrow_table()
        assert table.schema.field("b").type == pa.binary()

    def test_arrow_null_handling(self, cursor):
        cursor.execute("SELECT NULL AS n, 1 AS v")
        table = cursor.fetch_arrow_table()
        assert table.column("n")[0].as_py() is None
        assert table.column("v")[0].as_py() == 1

    def test_arrow_to_pandas(self, cursor):
        cursor.execute("SELECT 1 AS a, N'hello' AS b")
        table = cursor.fetch_arrow_table()
        df = table.to_pandas()
        assert df.shape == (1, 2)
        assert df["a"][0] == 1
        assert df["b"][0] == "hello"

    def test_arrow_large_result(self, cursor):
        """Test fetching many rows into Arrow."""
        cursor.execute("""
            SELECT TOP 10000 
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id,
                NEWID() AS uid
            FROM sys.all_columns a CROSS JOIN sys.all_columns b
        """)
        table = cursor.fetch_arrow_table()
        assert table.num_rows == 10000


class TestIngest:
    def test_ingest_create(self, cursor):
        cursor.execute("IF OBJECT_ID('test_ingest_1', 'U') IS NOT NULL DROP TABLE test_ingest_1")
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        cursor.adbc_ingest("test_ingest_1", table, mode="create")
        cursor.execute("SELECT * FROM test_ingest_1 ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == 1
        assert rows[0][1] == "a"
        # cleanup
        cursor.execute("DROP TABLE test_ingest_1")

    def test_ingest_replace(self, cursor):
        cursor.execute("IF OBJECT_ID('test_ingest_2', 'U') IS NOT NULL DROP TABLE test_ingest_2")
        table1 = pa.table({"x": [1, 2]})
        cursor.adbc_ingest("test_ingest_2", table1, mode="create")
        table2 = pa.table({"x": [10, 20, 30]})
        cursor.adbc_ingest("test_ingest_2", table2, mode="replace")
        cursor.execute("SELECT COUNT(*) FROM test_ingest_2")
        rows = cursor.fetchall()
        assert rows[0][0] == 3
        cursor.execute("DROP TABLE test_ingest_2")

    def test_ingest_append(self, cursor):
        cursor.execute("IF OBJECT_ID('test_ingest_3', 'U') IS NOT NULL DROP TABLE test_ingest_3")
        table1 = pa.table({"v": [1, 2]})
        cursor.adbc_ingest("test_ingest_3", table1, mode="create")
        table2 = pa.table({"v": [3, 4]})
        cursor.adbc_ingest("test_ingest_3", table2, mode="append")
        cursor.execute("SELECT COUNT(*) FROM test_ingest_3")
        rows = cursor.fetchall()
        assert rows[0][0] == 4
        cursor.execute("DROP TABLE test_ingest_3")


class TestPerformance:
    def test_arrow_vs_rows_performance(self, conn):
        """Compare Arrow-native fetch vs row-based fetch."""
        cur = conn.cursor()
        # Create a table with 100k rows
        cur.execute("""
            IF OBJECT_ID('perf_test', 'U') IS NOT NULL DROP TABLE perf_test;
            SELECT TOP 100000
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id,
                CAST(ABS(CHECKSUM(NEWID())) % 1000 AS FLOAT) AS value,
                CAST(NEWID() AS NVARCHAR(36)) AS name
            INTO perf_test
            FROM sys.all_columns a CROSS JOIN sys.all_columns b
        """)

        # Arrow fetch
        t0 = time.time()
        cur.execute("SELECT * FROM perf_test")
        table = cur.fetch_arrow_table()
        arrow_time = time.time() - t0
        assert table.num_rows == 100000

        # Row-based fetch
        t0 = time.time()
        cur.execute("SELECT * FROM perf_test")
        rows = cur.fetchall()
        rows_time = time.time() - t0
        assert len(rows) == 100000

        print(f"\nArrow fetch: {arrow_time:.3f}s, Row fetch: {rows_time:.3f}s")
        print(f"Arrow is {rows_time/arrow_time:.1f}x faster (or slower)")

        cur.execute("DROP TABLE perf_test")
        cur.close()
