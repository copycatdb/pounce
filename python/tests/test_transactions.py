"""Tests for transaction behavior."""

import pytest
from pounce import dbapi
from conftest import CONN_STR, drop_table, execute_sql


class TestAutocommit:
    def test_default_no_autocommit(self):
        conn = dbapi.connect(CONN_STR)
        assert conn.autocommit is False
        conn.close()

    def test_set_autocommit(self):
        conn = dbapi.connect(CONN_STR)
        conn.autocommit = True
        assert conn.autocommit is True
        conn.autocommit = False
        assert conn.autocommit is False
        conn.close()

    def test_autocommit_constructor(self):
        conn = dbapi.connect(CONN_STR, autocommit=True)
        assert conn.autocommit is True
        conn.close()


class TestCommitRollback:
    def test_commit(self):
        conn = dbapi.connect(CONN_STR)
        conn.autocommit = True
        drop_table(conn, "test_txn_commit")
        execute_sql(conn, "CREATE TABLE test_txn_commit (v INT)")

        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("INSERT INTO test_txn_commit VALUES (1)")
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM test_txn_commit")
        assert cur.fetchone()[0] == 1

        conn.autocommit = True
        drop_table(conn, "test_txn_commit")
        conn.close()

    def test_rollback(self):
        conn = dbapi.connect(CONN_STR)
        conn.autocommit = True
        drop_table(conn, "test_txn_rb")
        execute_sql(conn, "CREATE TABLE test_txn_rb (v INT)")

        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("INSERT INTO test_txn_rb VALUES (1)")
        conn.rollback()

        # After rollback, the insert should be gone
        conn.autocommit = True
        cur2 = conn.cursor()
        cur2.execute("SELECT COUNT(*) FROM test_txn_rb")
        assert cur2.fetchone()[0] == 0

        drop_table(conn, "test_txn_rb")
        conn.close()

    def test_multiple_commits(self):
        conn = dbapi.connect(CONN_STR)
        conn.autocommit = True
        drop_table(conn, "test_multi_commit")
        execute_sql(conn, "CREATE TABLE test_multi_commit (v INT)")

        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("INSERT INTO test_multi_commit VALUES (1)")
        conn.commit()
        cur.execute("INSERT INTO test_multi_commit VALUES (2)")
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM test_multi_commit")
        assert cur.fetchone()[0] == 2

        conn.autocommit = True
        drop_table(conn, "test_multi_commit")
        conn.close()

    def test_rollback_after_error(self):
        """Rollback should work after a failed statement."""
        conn = dbapi.connect(CONN_STR)
        conn.autocommit = True
        drop_table(conn, "test_rb_err")
        execute_sql(conn, "CREATE TABLE test_rb_err (v INT NOT NULL)")

        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("INSERT INTO test_rb_err VALUES (1)")
        try:
            cur.execute("INSERT INTO test_rb_err VALUES (NULL)")  # should fail
        except Exception:
            pass
        conn.rollback()

        conn.autocommit = True
        cur2 = conn.cursor()
        cur2.execute("SELECT COUNT(*) FROM test_rb_err")
        assert cur2.fetchone()[0] == 0

        drop_table(conn, "test_rb_err")
        conn.close()
