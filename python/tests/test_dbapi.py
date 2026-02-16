"""DB-API 2.0 compliance tests."""

import pytest
from pounce import dbapi
from conftest import CONN_STR, drop_table, execute_sql


class TestModuleAttributes:
    def test_apilevel(self):
        assert dbapi.apilevel == "2.0"

    def test_threadsafety(self):
        assert dbapi.threadsafety == 1

    def test_paramstyle(self):
        assert dbapi.paramstyle == "qmark"


class TestConnect:
    def test_connect_success(self):
        conn = dbapi.connect(CONN_STR)
        assert conn is not None
        conn.close()

    def test_connect_bad_string(self):
        with pytest.raises(Exception):
            dbapi.connect("Server=nonexistent_host_12345,9999;UID=x;PWD=x")

    def test_connect_context_manager(self):
        with dbapi.connect(CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    def test_close_idempotent(self):
        conn = dbapi.connect(CONN_STR)
        conn.close()
        conn.close()  # should not raise


class TestCursor:
    def test_cursor_from_closed_conn(self, conn):
        conn.close()
        with pytest.raises(dbapi.InterfaceError):
            conn.cursor()

    def test_execute_select(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT 42 AS answer")
        assert cur.description is not None
        assert cur.description[0][0] == "answer"
        assert cur.fetchone() == (42,)

    def test_fetchone_exhausted(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT 1 WHERE 1=0")
        assert cur.fetchone() is None

    def test_fetchall_empty(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT 1 WHERE 1=0")
        assert cur.fetchall() == []

    def test_fetchmany(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT n FROM (VALUES (1),(2),(3),(4),(5)) AS t(n)")
        rows = cur.fetchmany(3)
        assert len(rows) == 3
        rest = cur.fetchall()
        assert len(rest) == 2

    def test_rowcount_select(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT n FROM (VALUES (1),(2),(3)) AS t(n)")
        assert cur.rowcount == 3

    def test_rowcount_dml(self, conn):
        drop_table(conn, "test_rc")
        execute_sql(conn, "CREATE TABLE test_rc (id INT)")
        cur = conn.cursor()
        cur.execute("INSERT INTO test_rc VALUES (1)")
        assert cur.rowcount == 1
        drop_table(conn, "test_rc")

    def test_execute_with_params(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT ? + ?", [10, 20])
        assert cur.fetchone() == (30,)

    def test_execute_string_param(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT ?", ["hello"])
        assert cur.fetchone() == ("hello",)

    def test_execute_none_param(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT ?", [None])
        assert cur.fetchone() == (None,)

    def test_execute_bool_params(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT ?, ?", [True, False])
        assert cur.fetchone() == (1, 0)

    def test_executemany(self, conn):
        drop_table(conn, "test_em")
        execute_sql(conn, "CREATE TABLE test_em (val INT)")
        cur = conn.cursor()
        cur.executemany("INSERT INTO test_em VALUES (?)", [[1], [2], [3]])
        assert cur.rowcount == 3
        cur.execute("SELECT COUNT(*) FROM test_em")
        assert cur.fetchone() == (3,)
        drop_table(conn, "test_em")

    def test_iterator_protocol(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT n FROM (VALUES (1),(2),(3)) AS t(n)")
        rows = list(cur)
        assert rows == [(1,), (2,), (3,)]

    def test_cursor_context_manager(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    def test_closed_cursor_raises(self, conn):
        cur = conn.cursor()
        cur.close()
        with pytest.raises(dbapi.InterfaceError):
            cur.execute("SELECT 1")

    def test_fetch_arrow_table(self, conn):
        import pyarrow as pa
        cur = conn.cursor()
        cur.execute("SELECT 1 AS a, 'hello' AS b")
        table = cur.fetch_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert table.column_names == ["a", "b"]

    def test_fetch_arrow_no_result(self, conn):
        drop_table(conn, "test_fanr")
        execute_sql(conn, "CREATE TABLE test_fanr (id INT)")
        cur = conn.cursor()
        cur.execute("INSERT INTO test_fanr VALUES (1)")
        with pytest.raises(dbapi.ProgrammingError):
            cur.fetch_arrow_table()
        drop_table(conn, "test_fanr")

    def test_fetch_record_batch(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT n FROM (VALUES (1),(2),(3)) AS t(n)")
        reader = cur.fetch_record_batch()
        batches = list(reader)
        total = sum(b.num_rows for b in batches)
        assert total == 3

    def test_setinputsizes_noop(self, conn):
        cur = conn.cursor()
        cur.setinputsizes([])  # should not raise

    def test_setoutputsize_noop(self, conn):
        cur = conn.cursor()
        cur.setoutputsize(1000)  # should not raise


class TestExceptions:
    def test_bad_sql(self, conn):
        cur = conn.cursor()
        with pytest.raises(Exception):
            cur.execute("SELECTTTT INVALID SYNTAX")

    def test_exception_hierarchy(self):
        assert issubclass(dbapi.DatabaseError, dbapi.Error)
        assert issubclass(dbapi.OperationalError, dbapi.DatabaseError)
        assert issubclass(dbapi.IntegrityError, dbapi.DatabaseError)
        assert issubclass(dbapi.ProgrammingError, dbapi.DatabaseError)
        assert issubclass(dbapi.InterfaceError, dbapi.Error)
