"""Shared test fixtures for pounce tests."""

import os
import pytest

CONN_STR = os.environ.get(
    "POUNCE_TEST_CONN",
    "Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes",
)


@pytest.fixture
def conn():
    """Yield a fresh pounce connection, closed after test."""
    from pounce.dbapi import connect
    c = connect(CONN_STR)
    c.autocommit = True
    yield c
    c.close()


@pytest.fixture
def conn_str():
    return CONN_STR


def execute_sql(conn, sql):
    """Helper: execute SQL, ignore results."""
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()


def drop_table(conn, name):
    """Drop a table if it exists."""
    execute_sql(conn, f"IF OBJECT_ID('{name}', 'U') IS NOT NULL DROP TABLE {name}")
